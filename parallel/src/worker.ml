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

let make : type args message. (Config.t, args, message, _) make =
  fun ~create_producer ~create_consumer ~bin_args ~bin_message ->
  let module Worker = struct
    type worker_state_init_arg = unit
    type connection_state_init_arg = unit

    module T = struct
      type 'worker functions =
        { start : ('worker, unit, unit) Rpc_parallel.Function.t
        ; iter : ('worker, args, message) Rpc_parallel.Function.Direct_pipe.t
        ; stopped : ('worker, unit, unit Or_error.t) Rpc_parallel.Function.t
        }

      module Worker_state = struct
        type t = unit
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
        let init_connection_state ~connection:(_ : Rpc.Connection.t) ~worker_state:() () =
          return (Rpc_iterator.Connection_state.create ())
        ;;

        let init_worker_state () = return ()

        let functions =
          let implement_iter =
            Staged.unstage (Rpc_iterator.implement_iter ~create_producer ~create_consumer)
          in
          { start =
              C.of_async_one_way_rpc
                ~f:(fun ~worker_state:() ~conn_state () ->
                  Rpc_iterator.implement_start conn_state ())
                (Rpc_iterator.start_rpc ())
          ; iter =
              C.of_async_direct_pipe_rpc
                ~f:(fun ~worker_state:() ~conn_state args writer ->
                  implement_iter conn_state args writer >>| ok_exn)
                (Rpc_iterator.iter_rpc ~bin_args ~bin_message ())
          ; stopped =
              C.of_async_rpc
                ~f:(fun ~worker_state:() ~conn_state () ->
                  Rpc_iterator.implement_stopped conn_state ())
                (Rpc_iterator.stopped_rpc ())
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
            [%log.global.error "Failure" (name : string) (error : Error.t)])
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
          [%log.global.info "Log closed" (name : string) (reason : unit Or_error.t)]
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
        [%log.global.error
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
  end)
;;

module For_testing = struct
  let make : type args message. (unit, args, message, _) make =
    fun ~create_producer ~create_consumer ~bin_args ~bin_message ->
    let start_rpc = Rpc_iterator.start_rpc () in
    let iter_rpc = Rpc_iterator.iter_rpc ~bin_args ~bin_message () in
    let stopped_rpc = Rpc_iterator.stopped_rpc () in
    let implement_iter =
      Staged.unstage (Rpc_iterator.implement_iter ~create_producer ~create_consumer)
    in
    let client () =
      For_testing.Rpc_transport.client
        ~server_implementations:
          (Rpc.Implementations.create_exn
             ~implementations:
               [ Rpc.One_way.implement
                   start_rpc
                   Rpc_iterator.implement_start
                   ~on_exception:Close_connection
               ; Rpc.Pipe_rpc.implement_direct
                   iter_rpc
                   implement_iter
                   ~leave_open_on_exception:true
               ; Rpc.Rpc.implement stopped_rpc Rpc_iterator.implement_stopped
               ]
             ~on_unknown_rpc:`Raise
             ~on_exception:Log_on_background_exn)
        ~server_connection_state:(fun (_ : Rpc.Connection.t) ->
          Rpc_iterator.Connection_state.create ())
    in
    (module struct
      type config = unit
      type nonrec args = args
      type nonrec message = message
      type t = T

      let create () = return (Ok T)
      let close T = return ()

      let create_producer T args =
        Rpc_iterator.Global.of_pipe_rpc client args ~start_rpc ~iter_rpc ~stopped_rpc
      ;;
    end)
  ;;
end
