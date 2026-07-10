open! Core
open! Async
open! Import
include Worker_intf
include Types

module Config = struct
  type t =
    { how_to_run : Rpc_parallel.How_to_run.t
    ; name : string
    ; log_dir : string option
    }

  let create ?(how_to_run = Rpc_parallel.How_to_run.local) ?log_dir ~name () =
    { how_to_run; name; log_dir }
  ;;
end

let make
  : type args message state state_update.
    (Config.t, args, message, _, state, state_update) make
  =
  fun ~init
    ~create_producer
    ~create_consumer
    ~(state_updating : (state, state_update) State_updating.t)
    ~bin_args
    ~bin_message ->
  let update_worker_state, bin_state_update = State_updating.resolve state_updating in
  let module Worker = struct
    type worker_state_init_arg = unit
    type connection_state_init_arg = unit

    module T = struct
      type 'worker functions =
        { start : ('worker, unit, unit) Rpc_parallel.Function.t
        ; iter : ('worker, args, message) Rpc_parallel.Function.Direct_pipe.t
        ; stopped : ('worker, unit, unit Or_error.t) Rpc_parallel.Function.t
        ; update_state : ('worker, state_update, unit Or_error.t) Rpc_parallel.Function.t
        }

      module Worker_state = struct
        type t = state Rpc_iterator.Worker_state.t
        type init_arg = unit [@@deriving bin_io]
      end

      module Connection_state = struct
        type t = Rpc_iterator.Connection_state.t
        type init_arg = unit [@@deriving bin_io]
      end

      module Functions
          (C : Rpc_parallel.Creator
               with type connection_state := Connection_state.t
                and type worker_state := Worker_state.t) =
      struct
        let init_connection_state
          ~connection:(_ : Rpc.Connection.t)
          ~worker_state:(_ : Worker_state.t)
          ()
          =
          return (Rpc_iterator.Connection_state.create ())
        ;;

        let init_worker_state () = return (Rpc_iterator.Worker_state.create ())

        let functions =
          { start =
              C.of_async_one_way_rpc
                ~f:(fun ~worker_state:_ ~conn_state () ->
                  Rpc_iterator.implement_start conn_state)
                (Rpc_iterator.start_rpc ())
          ; iter =
              C.of_async_direct_pipe_rpc
                ~f:(fun ~worker_state ~conn_state args writer ->
                  Rpc_iterator.implement_iter
                    ~create_producer
                    ~create_consumer
                    ~init
                    ~worker_state
                    ~conn_state
                    args
                    writer
                  >>| ok_exn)
                (Rpc_iterator.iter_rpc ~bin_args ~bin_message ())
          ; stopped =
              C.of_async_rpc
                ~f:(fun ~worker_state:_ ~conn_state () ->
                  Rpc_iterator.implement_stopped conn_state)
                (Rpc_iterator.stopped_rpc ())
          ; update_state =
              C.of_async_rpc
                ~f:(fun ~worker_state ~conn_state:_ state_update ->
                  Rpc_iterator.implement_update_state
                    ~worker_state
                    ~update_worker_state
                    state_update)
                (Rpc_iterator.update_state_rpc ~bin_state_update ())
          }
        ;;
      end
    end

    include T
    include Rpc_parallel.Make (T)
  end
  in
  (module struct
    type config = Config.t
    type nonrec args = args
    type nonrec message = message
    type nonrec state_update = state_update

    type t =
      { worker : Worker.t
      ; log_closed : unit Deferred.t
      }

    let create { Config.how_to_run; name; log_dir } =
      let redirect which =
        match log_dir with
        | None -> `Dev_null
        | Some log_dir -> `File_append (log_dir ^/ [%string "%{name}.%{which}"])
      in
      match%bind
        Worker.spawn
          ~how:how_to_run
          ~name
          ~on_failure:(fun error ->
            [%log.error "Failure" (name : string) (error : Error.t)])
          ~shutdown_on:Heartbeater_connection_timeout
          ~redirect_stdout:(redirect "stdout")
          ~redirect_stderr:(redirect "stderr")
          ()
      with
      | Error _ as result -> return result
      | Ok worker ->
        let log_closed =
          let%map reason =
            Worker.Connection.with_client worker () ~f:(fun connection ->
              let%bind log =
                Worker.Connection.run_exn
                  connection
                  ~f:Rpc_parallel.Function.async_log
                  ~arg:()
              in
              Pipe.iter_without_pushback
                ~consumer:
                  (Pipe.add_consumer log ~downstream_flushed:(fun () ->
                     let%bind () = Log.Global.flushed () in
                     return `Ok))
                log
                ~f:(fun message ->
                  Log.Global.message (Log.Message.add_tags message [ "name", name ])))
          in
          [%log.info "Log closed" (name : string) (reason : unit Or_error.t)]
        in
        return (Ok { worker; log_closed })
    ;;

    let close { worker; log_closed } =
      match%bind Worker.shutdown worker with
      | Ok () -> log_closed
      | Error error ->
        (* If we failed to dispatch the shutdown RPC, then network communication with the
           worker is probably in a weird state or it's already dead, so waiting for
           [log_closed] is unlikely to accomplish much and may end up waiting for the
           heartbeat timeout (or potentially longer in really weird states?). If users run
           into this in practice and are sad to not have complete logs when something so
           unexpected happens, we can consider doing something different here. *)
        [%log.error
          "Failed to shutdown worker"
            (worker : Worker.t)
            ~is_log_closed:(Deferred.is_determined log_closed : bool)
            (error : Error.t)];
        return ()
    ;;

    let create_producer t args =
      Rpc_iterator.Global.of_direct_pipe
        (module Worker)
        t.worker
        args
        ~start_f:Worker.functions.start
        ~iter_f:Worker.functions.iter
        ~stopped_f:Worker.functions.stopped
    ;;

    let update_state t state_update =
      Worker.Connection.with_client t.worker () ~f:(fun connection ->
        Worker.Connection.run
          connection
          ~f:Worker.functions.update_state
          ~arg:state_update
        >>| Or_error.join)
      >>| Or_error.join
    ;;
  end)
;;

module For_testing = struct
  let make
    : type args message state state_update.
      (unit, args, message, _, state, state_update) make
    =
    fun ~init
      ~create_producer
      ~create_consumer
      ~(state_updating : (state, state_update) State_updating.t)
      ~bin_args
      ~bin_message ->
    let update_worker_state, bin_state_update = State_updating.resolve state_updating in
    let start_rpc = Rpc_iterator.start_rpc () in
    let iter_rpc = Rpc_iterator.iter_rpc ~bin_args ~bin_message () in
    let stopped_rpc = Rpc_iterator.stopped_rpc () in
    let update_state_rpc = Rpc_iterator.update_state_rpc ~bin_state_update () in
    let create_client worker_state =
      stage (fun () ->
        For_testing.Rpc_transport.client
          ~server_implementations:
            (Rpc.Implementations.create_exn
               ~implementations:
                 [ Rpc.One_way.implement
                     start_rpc
                     (fun conn_state () -> Rpc_iterator.implement_start conn_state)
                     ~on_exception:Close_connection
                 ; Rpc.Pipe_rpc.implement_direct iter_rpc (fun conn_state args writer ->
                     Rpc_iterator.implement_iter
                       ~create_producer
                       ~create_consumer
                       ~init
                       ~worker_state
                       ~conn_state
                       args
                       writer)
                 ; Rpc.Rpc.implement stopped_rpc (fun conn_state () ->
                     Rpc_iterator.implement_stopped conn_state)
                 ; Rpc.Rpc.implement
                     update_state_rpc
                     (fun (_ : Rpc_iterator.Connection_state.t) state_update ->
                        Rpc_iterator.implement_update_state
                          ~worker_state
                          ~update_worker_state
                          state_update)
                 ]
               ~on_unknown_rpc:`Raise
               ~on_exception:(Raise_to_monitor (Monitor.current ())))
          ~server_connection_state:(fun (_ : Rpc.Connection.t) ->
            Rpc_iterator.Connection_state.create ()))
    in
    (module struct
      type config = unit
      type nonrec args = args
      type nonrec message = message
      type nonrec state_update = state_update
      type t = { create_client : unit -> Rpc.Connection.t Or_error.t Deferred.t }

      let create () =
        let worker_state = Rpc_iterator.Worker_state.create () in
        return (Ok { create_client = unstage (create_client worker_state) })
      ;;

      let close (_ : t) = return ()

      let create_producer { create_client } args =
        Rpc_iterator.Global.of_pipe_rpc
          create_client
          args
          ~start_rpc
          ~iter_rpc
          ~stopped_rpc
      ;;

      let update_state { create_client } state_update =
        let%bind.Deferred.Or_error connection = create_client () in
        Monitor.protect
          (fun () ->
            Rpc.Rpc.dispatch update_state_rpc connection state_update >>| Or_error.join)
          ~finally:(fun () -> Rpc.Connection.close connection)
      ;;
    end)
  ;;
end
