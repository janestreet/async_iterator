open! Core
open! Async
open! Import

module Action = struct
  type t =
    | Stop
    | Continue
    | Wait of { pushback : unit Deferred.t [@globalized] }
  [@@deriving sexp_of]

  let globalize = Fn.id

  let of_maybe_pushback pushback =
    let pushback = Maybe_pushback.to_deferred pushback in
    if Deferred.is_determined pushback then Continue else Wait { pushback }
  ;;
end

type%template -'a f' = 'a -> Action.t [@@mode m = (global, local)]

module Producer = struct
  type -'f t' =
    { iter : f:'f -> stop:unit Deferred.Choice.t list -> unit Or_error.t Deferred.t }
  [@@unboxed]

  type%template +'a t = ('a f'[@mode m]) t' [@@mode m = (global, local)]
end

module Consumer = struct
  type +'f t' =
    { f : 'f
    ; stop : unit Deferred.Choice.t list
    }

  type%template -'a t = ('a f'[@mode m]) t' [@@mode m = (global, local)]
end

let start { Producer.iter } { Consumer.f; stop } = iter ~f ~stop

let upon' choices f =
  don't_wait_for (choose (List.map choices ~f:(Deferred.Choice.map ~f)))
;;

[%%template
[@@@mode.default m = (global, local)]

let create_producer' ~iter = { Producer.iter }

let simple_producer_of_expert ~iter ~(f : (_ f'[@mode m])) ~stop =
  iter
    ~f:(fun message ->
      match (f message : Action.t) with
      | Stop ->
        Ivar.fill_if_empty stop ();
        Maybe_pushback.unit
      | Continue -> Maybe_pushback.unit
      | Wait { pushback } -> Maybe_pushback.of_deferred pushback)
    ~stop:(Ivar.read stop)
;;

let create_producer ~iter =
  (create_producer' [@mode m]) ~iter:(fun ~f ~stop ->
    let stop_ivar = Ivar.create () in
    upon' stop (Ivar.fill_if_empty stop_ivar);
    (simple_producer_of_expert [@mode m]) ~iter ~f ~stop:stop_ivar)
;;

let create_producer_staged' ~iter ~start =
  let f_set_once = Set_once.create () in
  let f = lazy (Set_once.get_exn f_set_once) in
  let%bind.Deferred.Or_error state = iter ~f:(fun message -> (force f) message) in
  let producer =
    (create_producer' [@mode m]) ~iter:(fun ~f ~stop ->
      Set_once.set_exn f_set_once f;
      start state ~stop)
  in
  return (Ok producer)
;;

let create_producer_staged ~iter =
  (create_producer_staged' [@mode m])
    ~iter:(fun ~f ->
      let start_ivar = Ivar.create () in
      let stop_ivar = Ivar.create () in
      let%bind.Deferred.Or_error stopped =
        (simple_producer_of_expert [@mode m])
          ~iter:(iter ~start:(Ivar.read start_ivar))
          ~f
          ~stop:stop_ivar
      in
      return (Ok (start_ivar, stop_ivar, stopped)))
    ~start:(fun (start_ivar, stop_ivar, stopped) ~stop ->
      Ivar.fill_exn start_ivar ();
      upon' stop (Ivar.fill_if_empty stop_ivar);
      stopped)
;;

let create_consumer' ~f ?(stop = []) () = { Consumer.f; stop }

let create_consumer ~f ?stop () =
  match stop with
  | None ->
    (create_consumer' [@mode m])
      ~f:(fun message -> Action.of_maybe_pushback (f message))
      ()
  | Some stop ->
    (create_consumer' [@mode m])
      ~f:(fun message : Action.t ->
        if Deferred.is_determined stop then Stop else Action.of_maybe_pushback (f message))
      ~stop:[ choice stop (fun _ -> ()) ]
      ()
;;

let[@inline] gen_inspect ~f ~g x =
  f x;
  g x
;;

let[@inline] gen_filter ~f ~g x : Action.t = if f x then g x else Continue
let[@inline] gen_map ~f ~g x = g (f x)

let[@inline] gen_filter_map ~f ~g x : Action.t =
  match f x with
  | Some y -> g y
  | None -> Continue
;;

let[@inline] gen_concat_map ~f ~g x =
  let rec loop ys ~g ~pushbacks : Action.t =
    match ys with
    | [] ->
      if List.is_empty pushbacks
      then Continue
      else Wait { pushback = Deferred.all_unit pushbacks }
    | y :: ys ->
      (match (g y : Action.t) with
       | Stop -> Stop
       | Continue -> loop ys ~g ~pushbacks
       | Wait { pushback } -> loop ys ~g ~pushbacks:(pushback :: pushbacks))
  in
  loop (f x) ~g ~pushbacks:[]
;;

let inspect { Producer.iter } ~f =
  (create_producer' [@mode m]) ~iter:(fun ~f:g ~stop ->
    iter ~f:((gen_inspect [@mode m]) ~f ~g) ~stop)
;;

let filter { Producer.iter } ~f =
  (create_producer' [@mode m]) ~iter:(fun ~f:g ~stop ->
    iter ~f:((gen_filter [@mode m]) ~f ~g) ~stop)
;;

let map { Producer.iter } ~f =
  (create_producer' [@mode m]) ~iter:(fun ~f:g ~stop ->
    iter ~f:((gen_map [@mode m]) ~f ~g) ~stop)
;;

let filter_map { Producer.iter } ~f =
  (create_producer' [@mode m]) ~iter:(fun ~f:g ~stop ->
    iter ~f:((gen_filter_map [@mode m]) ~f ~g) ~stop)
;;

let concat_map { Producer.iter } ~f =
  (create_producer' [@mode m]) ~iter:(fun ~f:g ~stop ->
    iter ~f:((gen_concat_map [@mode m]) ~f ~g) ~stop)
;;

let contra_inspect { Consumer.f = g; stop } ~f =
  (create_consumer' [@mode m]) ~f:((gen_inspect [@mode m]) ~f ~g) ~stop ()
;;

let contra_filter { Consumer.f = g; stop } ~f =
  (create_consumer' [@mode m]) ~f:((gen_filter [@mode m]) ~f ~g) ~stop ()
;;

let contra_map { Consumer.f = g; stop } ~f =
  (create_consumer' [@mode m]) ~f:((gen_map [@mode m]) ~f ~g) ~stop ()
;;

let contra_filter_map { Consumer.f = g; stop } ~f =
  (create_consumer' [@mode m]) ~f:((gen_filter_map [@mode m]) ~f ~g) ~stop ()
;;

let contra_concat_map { Consumer.f = g; stop } ~f =
  (create_consumer' [@mode m]) ~f:((gen_concat_map [@mode m]) ~f ~g) ~stop ()
;;]

let gen_of_pipe_reader ~read_now ?(flushed = Pipe.Flushed.When_value_processed) reader =
  create_producer' ~iter:(fun ~f ~stop ->
    upon' stop (fun () -> Pipe.close_read reader);
    let consumer, downstream_flushed =
      match flushed with
      | When_value_read -> None, None
      | When_value_processed ->
        let downstream_flushed = ref (return `Ok) in
        let consumer =
          Pipe.add_consumer reader ~downstream_flushed:(fun () -> !downstream_flushed)
        in
        Some consumer, Some downstream_flushed
      | Consumer consumer -> Some consumer, None
    in
    Deferred.create (fun stopped ->
      let stop () =
        Option.iter downstream_flushed ~f:(fun downstream_flushed ->
          downstream_flushed := return `Reader_closed);
        Pipe.close_read reader;
        Ivar.fill_exn stopped (Ok ())
      in
      let rec continue () =
        match read_now ?consumer reader with
        | `Ok message ->
          let action = f message in
          Option.iter ~f:Pipe.Consumer.values_sent_downstream consumer;
          (match (action : Action.t) with
           | Stop -> stop ()
           | Continue -> continue ()
           | Wait { pushback } ->
             (match downstream_flushed with
              | None -> upon pushback continue
              | Some downstream_flushed ->
                downstream_flushed
                := Deferred.create (fun downstream_flushed ->
                     upon pushback (fun () ->
                       Ivar.fill_exn downstream_flushed `Ok;
                       continue ()))))
        | `Nothing_available ->
          upon (Pipe.values_available reader) (function
            | `Ok -> continue ()
            | `Eof -> stop ())
        | `Eof -> stop ()
      in
      continue ()))
;;

let of_pipe_reader ?flushed reader =
  gen_of_pipe_reader ~read_now:Pipe.read_now ?flushed reader
;;

let of_pipe_writer writer =
  create_consumer
    ~f:(Pipe.write writer >> Maybe_pushback.of_deferred)
    ~stop:(Pipe.closed writer)
    ()
;;

let gen_of_writer
  ~flushed
  ~closed
  ?(flush_every = 1)
  ?(on_flush = fun () -> Maybe_pushback.unit)
  writer
  ~write
  =
  let since_flush = ref 0 in
  create_consumer
    ~f:(fun message ->
      write writer message;
      incr since_flush;
      if !since_flush >= flush_every
      then (
        since_flush := 0;
        let%bind.Maybe_pushback () = on_flush () in
        flushed writer |> Maybe_pushback.of_deferred)
      else Maybe_pushback.unit)
    ~stop:(closed writer)
    ()
;;

let gen_of_direct_stream_writer ?flush_every ?on_flush writer ~write =
  gen_of_writer
    ~flushed:Rpc.Pipe_rpc.Direct_stream_writer.flushed
    ~closed:Rpc.Pipe_rpc.Direct_stream_writer.closed
    ?flush_every
    ?on_flush
    writer
    ~write
;;

let of_async_writer ?flush_every ?on_flush writer ~write =
  gen_of_writer
    ~flushed:Writer.flushed_or_failed_unit
    ~closed:Writer.close_started
    ?flush_every
    ?on_flush
    writer
    ~write
;;

let of_direct_stream_writer ?flush_every ?on_flush writer =
  gen_of_direct_stream_writer ?flush_every ?on_flush writer ~write:(fun writer message ->
    ignore
      (Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback writer message
       : [ `Ok | `Closed ]))
;;

let of_sequence sequence =
  (* We can ignore [stop] because sequences are synchronous, i.e. we're guaranteed to
       either generate another message or finish on every iteration, and are never in a
       state where we're waiting indefinitely for the next message and need some way to
       cancel out of it in order to stop. *)
  create_producer' ~iter:(fun ~f ~stop:(_ : unit Deferred.Choice.t list) ->
    Sequence.delayed_fold
      sequence
      ~init:()
      ~f:(fun () message ~k ->
        match (f message : Action.t) with
        | Stop -> return (Ok ())
        | Continue -> k ()
        | Wait { pushback } ->
          let%bind () = pushback in
          k ())
      ~finish:(fun () -> return (Ok ())))
;;

module Batched = struct
  module Producer = struct
    type 'a t = 'a Queue.t Producer.t
  end

  module Consumer = struct
    type 'a t = 'a Queue.t Consumer.t
  end

  let inspect t ~f = inspect t ~f:(Queue.iter ~f)
  and contra_inspect t ~f = contra_inspect t ~f:(Queue.iter ~f)
  and filter t ~f = inspect t ~f:(Queue.filter_inplace ~f)
  and contra_filter t ~f = contra_inspect t ~f:(Queue.filter_inplace ~f)
  and map t ~f = map t ~f:(Queue.map ~f)
  and contra_map t ~f = contra_map t ~f:(Queue.map ~f)
  and filter_map t ~f = map t ~f:(Queue.filter_map ~f)
  and contra_filter_map t ~f = contra_map t ~f:(Queue.filter_map ~f)
  and concat_map t ~f = map t ~f:(Queue.concat_map ~f)
  and contra_concat_map t ~f = contra_map t ~f:(Queue.concat_map ~f)

  let of_pipe_writer writer =
    create_consumer
      ~f:(fun messages ->
        Pipe.transfer_in writer ~from:messages |> Maybe_pushback.of_deferred)
      ~stop:(Pipe.closed writer)
      ()
  ;;

  let of_async_writer ?flush_every ?on_flush writer ~write =
    of_async_writer ?flush_every ?on_flush writer ~write:(fun writer messages ->
      Queue.iter ~f:(write writer) messages)
  ;;

  let of_direct_stream_writer ?flush_every ?on_flush writer =
    gen_of_direct_stream_writer
      ?flush_every
      ?on_flush
      writer
      ~write:(fun writer messages ->
        Queue.iter messages ~f:(fun message ->
          ignore
            (Rpc.Pipe_rpc.Direct_stream_writer.write_without_pushback writer message
             : [ `Ok | `Closed ])))
  ;;

  let of_pipe_reader ?flushed reader =
    gen_of_pipe_reader ~read_now:(Pipe.read_now' ?max_queue_length:None) ?flushed reader
  ;;
end

let create_producer_with_resource acquire ~close ~create =
  let close resource = close resource >>| Or_error.tag ~tag:"failed to close resource" in
  match%bind acquire () with
  | Ok resource ->
    (match%bind create resource with
     | Ok { Producer.iter } ->
       return
         (Ok
            { Producer.iter =
                (fun ~f ~stop ->
                  let%bind stopped = iter ~f ~stop in
                  let%bind closed = close resource in
                  return (Or_error.combine_errors_unit [ stopped; closed ]))
            })
     | Error create_error ->
       let%bind closed = close resource in
       return
         (Error (Error.of_list (create_error :: Option.to_list (Result.error closed)))))
  | Error _ as result -> return result
;;

[%%template
[@@@mode.default m = (global, local)]

let abort producer =
  start
    producer
    ((create_consumer [@mode m]) ~f:(fun _ -> Maybe_pushback.unit) ~stop:(return ()) ())
;;

let add_start { Producer.iter } ~start =
  { Producer.iter =
      (fun ~f ~stop ->
        match%bind
          choose
            (choice start (fun _ -> `Start)
             :: List.map stop ~f:(Deferred.Choice.map ~f:(fun () -> `Stop)))
        with
        | `Start -> iter ~f ~stop
        | `Stop -> (abort [@mode m]) { iter })
  }
;;

let add_stop { Consumer.f; stop = stop_choices } ~stop =
  { Consumer.f =
      (fun message : Action.t -> if Deferred.is_determined stop then Stop else f message)
  ; stop = choice stop (fun _ -> ()) :: stop_choices
  }
;;

let add_stop' { Consumer.f; stop = stop_choices } ~stop =
  { Consumer.f; stop = List.rev_append stop stop_choices }
;;]

let coerce_producer producer =
  (producer
    : [%template: (_ Producer.t[@mode global])]
    :> [%template: (_ Producer.t[@mode local])])
;;

let coerce_consumer consumer =
  (consumer
    : [%template: (_ Consumer.t[@mode local])]
    :> [%template: (_ Consumer.t[@mode global])])
;;

let%template unwrap_producer { Producer.iter } =
  (create_producer' [@mode global]) ~iter:(fun ~f ~stop ->
    iter ~f:(fun { global = message } -> f message) ~stop)
;;

let%template wrap_consumer { Consumer.f; stop } =
  (create_consumer' [@mode local]) ~f:(fun { global = message } -> f message) ~stop ()
;;

let pre_sequence { Consumer.f; stop } =
  let pushback_ref = ref Maybe_pushback.unit in
  let is_stopped = ref false in
  { Consumer.f =
      (fun message : Action.t ->
        if !is_stopped
        then Stop
        else if Deferred.is_determined (!pushback_ref :> unit Deferred.t)
        then (
          (* If the deferred is already determined we can follow a fast-path, in
             particular this should be the case if our producer respected the pushback.

             This avoids some allocations, and also caml_modify when our call to the inner
             consumer doesn't need to pushback either. *)
          let result = f message in
          (* If the function is stopping or pushing back we must remember it. *)
          (match (result : Action.t) with
           | Stop -> is_stopped := true
           | Continue -> ()
           | Wait { pushback } -> pushback_ref := Maybe_pushback.of_deferred pushback);
          result)
        else (
          Ref.replace pushback_ref (fun pushback ->
            (pushback :> unit Deferred.t)
            |> Deferred.bind ~f:(fun () ->
              if !is_stopped
              then Deferred.unit
              else (
                match (f message : Action.t) with
                | Stop ->
                  is_stopped := true;
                  Deferred.unit
                | Continue -> Deferred.unit
                | Wait { pushback } -> pushback))
            |> Maybe_pushback.of_deferred);
          Action.of_maybe_pushback !pushback_ref))
  ; stop
  }
;;

let post_sequence { Consumer.f; stop } =
  let pushback_ref = ref Maybe_pushback.unit in
  { Consumer.f =
      (fun message : Action.t ->
        match (f message : Action.t) with
        | Stop -> Stop
        | Continue -> Action.of_maybe_pushback !pushback_ref
        | Wait { pushback } ->
          Ref.replace
            pushback_ref
            (Maybe_pushback.bind ~f:(fun () -> Maybe_pushback.of_deferred pushback));
          Action.of_maybe_pushback !pushback_ref)
  ; stop
  }
;;

module Helpers = struct
  let upon' = upon'
end
