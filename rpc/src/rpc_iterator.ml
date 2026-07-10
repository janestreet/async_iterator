open! Core
open! Async
open! Import

module Connection_state = struct
  type t =
    { start : unit Ivar.t
    ; stopped : unit Or_error.t Ivar.t
    }

  let create () = { start = Ivar.create (); stopped = Ivar.create () }
  let fill_start_exn t = Ivar.fill_exn t.start ()
  let read_start t = Ivar.read t.start
  let fill_stopped_exn t reason = Ivar.fill_exn t.stopped reason
  let read_stopped t = Ivar.read t.stopped
end

module Worker_state = struct
  type 'a t = 'a or_null ref

  let create () = ref Null
end

let start_rpc ?(name = "start") ?(version = 0) () =
  Rpc.One_way.create ~name ~version ~bin_msg:bin_unit
;;

let iter_rpc ?(name = "iter") ?(version = 0) ~bin_args ~bin_message () =
  (* [client_pushes_back] is irrelevant when using [implement_direct], [dispatch_iter]. *)
  Rpc.Pipe_rpc.create
    ~name
    ~version
    ~bin_query:bin_args
    ~bin_response:bin_message
    ~bin_error:Error.bin_t
    ()
;;

let stopped_rpc ?(name = "stopped") ?(version = 0) () =
  Rpc.Rpc.create
    ~name
    ~version
    ~bin_query:bin_unit
    ~bin_response:(Or_error.bin_t bin_unit)
    ~include_in_error_count:Or_error
;;

let update_state_rpc ?(name = "update_state") ?(version = 0) ~bin_state_update () =
  Rpc.Rpc.create
    ~name
    ~version
    ~bin_query:bin_state_update
    ~bin_response:(Or_error.bin_t bin_unit)
    ~include_in_error_count:Or_error
;;

let implement_start connection_state = Connection_state.fill_start_exn connection_state

let implement_iter
  ~create_producer
  ~create_consumer
  ~init
  ~worker_state
  ~conn_state
  args
  writer
  =
  let%bind.Deferred.Or_error state = init args in
  worker_state := This state;
  (* Creating either the producer or consumer might fail. If we created the producer
     first, it'd be prudent to [Iterator.abort] it if the consumer has an error, in order
     to clean up any resources. Instead, we can create the consumer first, since it's
     unlikely to have things to clean up, and there's not an equivalent [abort]-like
     operation for consumers anyway. (One reason for this is to support
     producer-to-consumer mappings which are many-to-one - if a consumer is screwed up, we
     definitely can't use its producer, but if a producer is screwed up, we might still
     want to do things with its consumer.) *)
  let clean_up_on_error () =
    worker_state := Null;
    Rpc.Pipe_rpc.Direct_stream_writer.close writer
  in
  let%bind result =
    let%bind.Deferred.Or_error consumer = create_consumer state args writer in
    let%bind.Deferred.Or_error producer = create_producer args in
    let producer =
      Iterator.add_start producer ~start:(Connection_state.read_start conn_state)
    in
    upon
      (Monitor.try_with_join_or_error (fun () ->
         Iterator.start_unsequenced producer consumer))
      (fun stopped ->
        clean_up_on_error ();
        Connection_state.fill_stopped_exn conn_state stopped);
    return (Ok ())
  in
  Or_error.iter_error result ~f:(fun (_ : Error.t) -> clean_up_on_error ());
  return result
;;

let implement_stopped connection_state = Connection_state.read_stopped connection_state

let implement_update_state ~worker_state ~update_worker_state state_update =
  match !worker_state with
  | Null ->
    Deferred.Or_error.error_string
      "Cannot update worker state: state not initialized (worker has not started \
       processing yet)"
  | This state -> update_worker_state state state_update
;;

module Global = struct
  let gen_of_pipe_rpc
    ~start_rpc
    ~dispatch_one_way
    ~iter_rpc
    ~dispatch_iter
    ~abort
    ~stopped_rpc
    ~dispatch_rpc
    client
    ~close
    query
    =
    Iterator.create_producer_with_resource
      client
      ~close:(close >> Deferred.ok)
      ~create:(fun connection ->
        Iterator.create_producer_staged'
          ~iter:(fun ~f ->
            let stop_requested = Ivar.create () in
            let this_side_stopped = Ivar.create () in
            let%bind.Deferred.Or_error id =
              dispatch_iter iter_rpc connection query ~f:(function
                | Rpc.Pipe_rpc.Pipe_message.Update message ->
                  (match f message with
                   | Stop ->
                     Ivar.fill_if_empty stop_requested ();
                     Rpc.Pipe_rpc.Pipe_response.Continue
                   | Continue -> Continue
                   | Wait { pushback } -> Wait pushback)
                | Closed reason ->
                  Ivar.fill_if_empty
                    this_side_stopped
                    (match reason with
                     | `By_remote_side -> Ok ()
                     | `Error error -> Error (Error.tag error ~tag:"pipe rpc closed"));
                  Continue)
            in
            return
              (Ok (connection, id, Ivar.read stop_requested, Ivar.read this_side_stopped)))
          ~start:(fun (connection, id, stop_requested, this_side_stopped) ~stop ->
            let started =
              match%bind dispatch_one_way start_rpc connection () with
              | Error error ->
                abort iter_rpc connection id;
                return (Error (Error.tag error ~tag:"failed to start"))
              | Ok () -> return (Ok ())
            in
            Iterator.Helpers.upon' (choice stop_requested Fn.id :: stop) (fun () ->
              abort iter_rpc connection id);
            let remote_stopped =
              match%bind dispatch_rpc stopped_rpc connection () with
              | Error error ->
                return (Error (Error.tag error ~tag:"unknown remote stop reason"))
              | Ok reason -> return (Or_error.tag reason ~tag:"remote stopped")
            in
            Deferred.Or_error.combine_errors_unit
              [ started; this_side_stopped; remote_stopped ]))
  ;;

  let of_pipe_rpc client query ~start_rpc ~iter_rpc ~stopped_rpc =
    gen_of_pipe_rpc
      ~start_rpc
      ~dispatch_one_way:(fun rpc connection query ->
        Rpc.One_way.dispatch rpc connection query |> return)
      ~iter_rpc
      ~dispatch_iter:(fun rpc connection query ~f ->
        Rpc.Pipe_rpc.dispatch_iter rpc connection query ~f >>| Or_error.join)
      ~abort:Rpc.Pipe_rpc.abort
      ~stopped_rpc
      ~dispatch_rpc:Rpc.Rpc.dispatch
      client
      ~close:Rpc.Connection.close
      query
  ;;

  let of_direct_pipe
    (type worker)
    (module Worker : Rpc_parallel.Worker
      with type t = worker
       and type connection_state_init_arg = unit)
    worker
    query
    ~start_f
    ~iter_f
    ~stopped_f
    =
    (* [Rpc_parallel] doesn't really distinguish between one- and two-way RPCs. *)
    let dispatch_rpc function_ connection query =
      Worker.Connection.run connection ~f:function_ ~arg:query
    in
    gen_of_pipe_rpc
      ~start_rpc:start_f
      ~dispatch_one_way:dispatch_rpc
      ~iter_rpc:iter_f
      ~dispatch_iter:(fun function_ connection query ~f ->
        Worker.Connection.run connection ~f:function_ ~arg:(query, f))
      ~abort:(fun (_ : _ Rpc_parallel.Function.Direct_pipe.t) connection id ->
        Worker.Connection.abort connection ~id)
      ~stopped_rpc:stopped_f
      ~dispatch_rpc
      (Worker.Connection.client worker)
      ~close:Worker.Connection.close
      query
  ;;
end
