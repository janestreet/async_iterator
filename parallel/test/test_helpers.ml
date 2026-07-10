open! Core
open! Async
open! Import

module Info = struct
  type t =
    { stop : int
    ; size_budget : int option
    }
  [@@deriving bin_io]

  let create ?(stop = 5) ?size_budget () = { stop; size_budget }
end

module Args = struct
  type t =
    { info : Info.t
    ; worker : int
    }
  [@@deriving bin_io]
end

module Message = struct
  type t =
    { worker : int
    ; i : int
    }
  [@@deriving bin_io, equal, sexp_of]
end

let create_producer { Args.info = { stop; size_budget }; worker } =
  let reader =
    Sequence.range 0 stop
    |> Sequence.map ~f:(fun i -> { Message.worker; i })
    |> Pipe.of_sequence
  in
  Option.iter size_budget ~f:(Pipe.set_size_budget reader);
  return (Ok (Iterator.Batched.of_pipe_reader reader))
;;

let consumer ?stop_after ?(pushback = fun (_ : Message.t) -> Maybe_pushback.unit) () =
  let stop = Ivar.create () in
  Iterator.create_consumer
    ~f:(fun message ->
      printf !"%{sexp: Message.t}\n" message;
      Option.iter stop_after ~f:(fun stop_after ->
        if Message.equal message stop_after then Ivar.fill_exn stop ());
      pushback message)
    ~stop:(Ivar.read stop)
    ()
;;

(** 3 because 1 is degenerate and some async things are weird with only 2, e.g. which
    order ivar handlers run in, an optimization for [choose], etc. *)
let number_of_workers = 3

let with_worker_pool worker_module workers ~f =
  let%bind pool = Worker_pool.create worker_module workers >>| ok_exn in
  Monitor.protect (fun () -> f pool) ~finally:(fun () -> Worker_pool.close pool)
;;

let default_workers ?(stop = 5) () =
  Nonempty_list.init number_of_workers ~f:(fun (_ : int) -> (), Info.create ~stop ())
;;

let create_producer_from_worker_pool pool =
  Worker_pool.create_producer pool ~args:(fun ~worker _ info -> { Args.info; worker })
  >>| ok_exn
;;

let iter_with
  ?(create_producer = create_producer)
  ?(bin_message = Message.bin_t)
  ?(info = fun (_ : int) -> Info.create ())
  consumer
  =
  let worker_module =
    Worker.make
      ~init:(fun (_ : Args.t) -> return (Ok ()))
      ~create_producer
      ~create_consumer:(fun () (_ : Args.t) writer ->
        return (Ok (Iterator.Batched.of_direct_stream_writer writer)))
      ~state_updating:Not_using
      ~bin_args:Args.bin_t
      ~bin_message
  in
  let workers = Nonempty_list.init number_of_workers ~f:(fun worker -> (), info worker) in
  with_worker_pool worker_module workers ~f:(fun pool ->
    match%bind
      Worker_pool.create_producer pool ~args:(fun ~worker () info ->
        { Args.info; worker })
    with
    | Error error ->
      print_s [%message "Failed to start" (error : Error.t)];
      return ()
    | Ok producer ->
      let%bind reason = Iterator.start_unsequenced producer consumer in
      print_s [%message "Stopped" (reason : unit Or_error.t)];
      return ())
;;

module Worker_state = struct
  type t = { mutable offset : int }
end

let make_state_worker
  ?(init = fun (_ : Args.t) -> return (Ok { Worker_state.offset = 0 }))
  ?(create_producer = create_producer)
  ()
  =
  let update_worker_state (state : Worker_state.t) new_offset =
    state.offset <- new_offset;
    return (Ok ())
  in
  Worker.make
    ~init
    ~create_producer
    ~create_consumer:(fun (state : Worker_state.t) (_ : Args.t) writer ->
      return
        (Ok
           (Iterator.Batched.contra_map
              (Iterator.Batched.of_direct_stream_writer writer)
              ~f:(fun (message : Message.t) ->
                { message with i = message.i + state.offset }))))
    ~state_updating:(Using { update_worker_state; bin_state_update = bin_int })
    ~bin_args:Args.bin_t
    ~bin_message:Message.bin_t
;;
