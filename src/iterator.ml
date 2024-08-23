open! Core
open! Async
open! Import
include Iterator_intf

module Action = struct
  type t =
    | Stop
    | Continue
    | Wait of { global_ pushback : unit Deferred.t }
  [@@deriving globalize, sexp_of]

  let of_maybe_pushback pushback = exclave_
    let pushback = Maybe_pushback.to_deferred pushback in
    if Deferred.is_determined pushback then Continue else Wait { pushback }
  ;;
end

module Producer = struct
  type 'message t =
    { iter :
        f:(local_ 'message -> local_ Action.t)
        -> stop:unit Deferred.Choice.t list
        -> unit Or_error.t Deferred.t
    }
  [@@unboxed]
end

module Consumer = struct
  type 'message t =
    { f : local_ 'message -> local_ Action.t
    ; stop : unit Deferred.Choice.t list
    }
end

module type S =
  S
  with type action := Action.t
   and type 'message producer := 'message Producer.t
   and type 'message consumer := 'message Consumer.t

module type S_global =
  S_global
  with type action := Action.t
   and type 'message producer := 'message Producer.t
   and type 'message consumer := 'message Consumer.t

let start { Producer.iter } { Consumer.f; stop } = iter ~f ~stop

let upon' choices f =
  don't_wait_for (choose (List.map choices ~f:(Deferred.Choice.map ~f)))
;;

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

module Local = struct
  module Producer = Producer
  module Consumer = Consumer

  let create_producer' ~iter = { Producer.iter }

  let wrap_producer ~iter ~f ~stop =
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
    create_producer' ~iter:(fun ~f ~stop ->
      let stop_ivar = Ivar.create () in
      upon' stop (Ivar.fill_if_empty stop_ivar);
      wrap_producer ~iter ~f ~stop:stop_ivar)
  ;;

  let create_producer_staged' ~iter ~start =
    let f_set_once = Set_once.create () in
    let f = lazy (Set_once.get_exn f_set_once [%here]) in
    let%bind.Deferred.Or_error state =
      iter ~f:(fun message -> exclave_ (force f) message)
    in
    let producer =
      create_producer' ~iter:(fun ~f ~stop ->
        Set_once.set_exn f_set_once [%here] f;
        start state ~stop)
    in
    return (Ok producer)
  ;;

  let create_producer_staged ~iter =
    create_producer_staged'
      ~iter:(fun ~f ->
        let start_ivar = Ivar.create () in
        let stop_ivar = Ivar.create () in
        let%bind.Deferred.Or_error stopped =
          wrap_producer ~iter:(iter ~start:(Ivar.read start_ivar)) ~f ~stop:stop_ivar
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
      create_consumer'
        ~f:(fun message -> exclave_ Action.of_maybe_pushback (f message))
        ()
    | Some stop ->
      create_consumer'
        ~f:(fun message ->
          exclave_
          if Deferred.is_determined stop
          then Stop
          else Action.of_maybe_pushback (f message))
        ~stop:[ choice stop (fun _ -> ()) ]
        ()
  ;;

  let gen_inspect ~f ~g message = exclave_
    f message;
    g message
  ;;

  let inspect { Producer.iter } ~f =
    { Producer.iter = (fun ~f:g -> iter ~f:(gen_inspect ~f ~g)) }
  ;;

  let contra_inspect { Consumer.f = g; stop } ~f =
    { Consumer.f = gen_inspect ~f ~g; stop }
  ;;

  let gen_filter ~f ~g message : Action.t = exclave_
    if f message then g message else Continue
  ;;

  let filter { Producer.iter } ~f =
    { Producer.iter = (fun ~f:g -> iter ~f:(gen_filter ~f ~g)) }
  ;;

  let contra_filter { Consumer.f = g; stop } ~f = { Consumer.f = gen_filter ~f ~g; stop }
  let gen_map ~f ~g message = exclave_ g (f message)
  let map { Producer.iter } ~f = { Producer.iter = (fun ~f:g -> iter ~f:(gen_map ~f ~g)) }
  let contra_map { Consumer.f = g; stop } ~f = { Consumer.f = gen_map ~f ~g; stop }

  let gen_filter_map ~f ~g a : Action.t = exclave_
    match f a with
    | None -> Continue
    | Some b -> g b
  ;;

  let filter_map { Producer.iter } ~f =
    { Producer.iter = (fun ~f:g -> iter ~f:(gen_filter_map ~f ~g)) }
  ;;

  let contra_filter_map { Consumer.f = g; stop } ~f =
    { Consumer.f = gen_filter_map ~f ~g; stop }
  ;;

  let gen_concat_map ~f ~g a = exclave_
    let rec loop bs ~g ~pushbacks : Action.t = exclave_
      match bs with
      | [] ->
        if List.is_empty pushbacks
        then Continue
        else Wait { pushback = Deferred.all_unit pushbacks }
      | b :: bs ->
        (match (g b : Action.t) with
         | Stop -> Stop
         | Continue -> loop bs ~g ~pushbacks
         | Wait { pushback } -> loop bs ~g ~pushbacks:(pushback :: pushbacks))
    in
    loop (f a) ~g ~pushbacks:[]
  ;;

  let concat_map { Producer.iter } ~f =
    { Producer.iter = (fun ~f:g -> iter ~f:(gen_concat_map ~f ~g)) }
  ;;

  let contra_concat_map { Consumer.f = g; stop } ~f =
    { Consumer.f = gen_concat_map ~f ~g; stop }
  ;;
end

module Global = struct
  module Producer = struct
    type 'message t = 'message Modes.Global.t Local.Producer.t
  end

  module Consumer = struct
    type 'message t = 'message Modes.Global.t Local.Consumer.t
  end

  let create_producer ~iter =
    Local.create_producer ~iter:(fun ~f ~stop ->
      iter ~f:(fun message -> f { global = message }) ~stop)
  ;;

  let create_producer' ~iter =
    Local.create_producer' ~iter:(fun ~f ~stop ->
      iter ~f:(fun message -> exclave_ f { global = message }) ~stop)
  ;;

  let create_producer_staged ~iter =
    Local.create_producer_staged ~iter:(fun ~start ~f ~stop ->
      iter ~start ~f:(fun message -> f { global = message }) ~stop)
  ;;

  let create_producer_staged' ~iter ~start =
    Local.create_producer_staged'
      ~iter:(fun ~f -> iter ~f:(fun message -> exclave_ f { global = message }))
      ~start
  ;;

  let create_consumer ~f ?stop () =
    Local.create_consumer ~f:(fun { global = message } -> f message) ?stop ()
  ;;

  let create_consumer' ~f ?stop () =
    Local.create_consumer' ~f:(fun { global = message } -> exclave_ f message) ?stop ()
  ;;

  let gen_inspect ~f { global = message } = f message
  let inspect t ~f = Local.inspect t ~f:(gen_inspect ~f)
  let contra_inspect t ~f = Local.contra_inspect t ~f:(gen_inspect ~f)
  let gen_filter ~f { global = message } = f message
  let filter t ~f = Local.filter t ~f:(gen_filter ~f)
  let contra_filter t ~f = Local.contra_filter t ~f:(gen_filter ~f)
  let gen_map ~f { global = message } = { global = f message }
  let map t ~f = Local.map t ~f:(gen_map ~f)
  let contra_map t ~f = Local.contra_map t ~f:(gen_map ~f)

  let gen_filter_map ~f { global = a } = exclave_
    match f a with
    | Some b -> Some { global = b }
    | None -> None
  ;;

  let filter_map t ~f = Local.filter_map t ~f:(gen_filter_map ~f)
  let contra_filter_map t ~f = Local.contra_filter_map t ~f:(gen_filter_map ~f)

  let gen_concat_map ~f { global = a } =
    (* An [Obj.magic : 'b list -> 'b Gel.t list] would arguably suffice here, but this
       seems cheap enough. We could also reimplement [Local.concat_map] but for global
       values, but this keeps things simple and will eventually be made unnecessary by
       mode polymorphism, or allocate the ['b Gel.t list] on the stack, but there's
       currently a stack size limit that using the heap circumvents. *)
    let[@tail_mod_cons] rec loop = function
      | b :: bs -> { global = b } :: loop bs
      | [] -> []
    in
    loop (f a)
  ;;

  let concat_map t ~f = Local.concat_map t ~f:(gen_concat_map ~f)
  let contra_concat_map t ~f = Local.contra_concat_map t ~f:(gen_concat_map ~f)

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
            (match action with
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
    gen_of_direct_stream_writer
      ?flush_every
      ?on_flush
      writer
      ~write:(fun writer message ->
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
          match f message with
          | Stop -> return (Ok ())
          | Continue -> k ()
          | Wait { pushback } ->
            let%bind () = pushback in
            k ())
        ~finish:(fun () -> return (Ok ())))
  ;;
end

module Batched = struct
  module Producer = struct
    type 'message t = 'message Queue.t Global.Producer.t
  end

  module Consumer = struct
    type 'message t = 'message Queue.t Global.Consumer.t
  end

  let create_producer ~iter = Global.create_producer ~iter
  let create_producer' ~iter = Global.create_producer' ~iter
  let create_producer_staged ~iter = Global.create_producer_staged ~iter
  let create_producer_staged' ~iter ~start = Global.create_producer_staged' ~iter ~start
  let create_consumer ~f ?stop () = Global.create_consumer ~f ?stop ()
  let create_consumer' ~f ?stop () = Global.create_consumer' ~f ?stop ()
  let inspect t ~f = Global.inspect t ~f:(Queue.iter ~f)
  let contra_inspect t ~f = Global.contra_inspect t ~f:(Queue.iter ~f)
  let filter t ~f = Global.inspect t ~f:(Queue.filter_inplace ~f)
  let contra_filter t ~f = Global.contra_inspect t ~f:(Queue.filter_inplace ~f)
  let map t ~f = Global.map t ~f:(Queue.map ~f)
  let contra_map t ~f = Global.contra_map t ~f:(Queue.map ~f)
  let filter_map t ~f = Global.map t ~f:(Queue.filter_map ~f)
  let contra_filter_map t ~f = Global.contra_map t ~f:(Queue.filter_map ~f)
  let concat_map t ~f = Global.map t ~f:(Queue.concat_map ~f)
  let contra_concat_map t ~f = Global.contra_map t ~f:(Queue.concat_map ~f)

  let of_pipe_writer writer =
    create_consumer
      ~f:(fun messages ->
        Pipe.transfer_in writer ~from:messages |> Maybe_pushback.of_deferred)
      ~stop:(Pipe.closed writer)
      ()
  ;;

  let of_async_writer ?flush_every ?on_flush writer ~write =
    Global.of_async_writer ?flush_every ?on_flush writer ~write:(fun writer messages ->
      Queue.iter ~f:(write writer) messages)
  ;;

  let of_direct_stream_writer ?flush_every ?on_flush writer =
    Global.gen_of_direct_stream_writer
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
    Global.gen_of_pipe_reader
      ~read_now:(Pipe.read_now' ?max_queue_length:None)
      ?flushed
      reader
  ;;
end

let abort producer =
  start
    producer
    (Local.create_consumer ~f:(fun _ -> Maybe_pushback.unit) ~stop:(return ()) ())
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
        | `Stop -> abort { iter })
  }
;;

let add_stop { Consumer.f; stop = stop_choices } ~stop =
  { Consumer.f =
      (fun message -> exclave_ if Deferred.is_determined stop then Stop else f message)
  ; stop = choice stop (fun _ -> ()) :: stop_choices
  }
;;

let add_stop' { Consumer.f; stop = stop_choices } ~stop =
  { Consumer.f; stop = List.rev_append stop stop_choices }
;;

let coerce_producer t = Local.map t ~f:(fun { global = message } -> message)
let coerce_consumer t = Local.contra_map t ~f:(fun { global = message } -> message)

let pre_sequence { Consumer.f; stop } =
  let pushback_ref = ref Maybe_pushback.unit in
  let is_stopped = ref false in
  { Consumer.f =
      (fun { global = message } -> exclave_
        if !is_stopped
        then Stop
        else (
          Ref.replace pushback_ref (fun pushback ->
            let%bind.Maybe_pushback () = pushback in
            if !is_stopped
            then Maybe_pushback.unit
            else (
              match f { global = message } with
              | Stop ->
                is_stopped := true;
                Maybe_pushback.unit
              | Continue -> Maybe_pushback.unit
              | Wait { pushback } -> Maybe_pushback.of_deferred pushback));
          Action.of_maybe_pushback !pushback_ref))
  ; stop
  }
;;

let post_sequence { Consumer.f; stop } =
  let pushback_ref = ref Maybe_pushback.unit in
  let is_stopped = ref false in
  { Consumer.f =
      (fun message -> exclave_
        if !is_stopped
        then Stop
        else (
          match f message with
          | Stop -> Stop
          | Continue -> Action.of_maybe_pushback !pushback_ref
          | Wait { pushback } ->
            Ref.replace
              pushback_ref
              (Maybe_pushback.bind ~f:(fun () -> Maybe_pushback.of_deferred pushback));
            Action.of_maybe_pushback !pushback_ref))
  ; stop
  }
;;

module Helpers = struct
  let upon' = upon'
end
