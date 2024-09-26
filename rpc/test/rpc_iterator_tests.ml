open! Core
open! Async
open! Import

(* 2023-06-26:  A lot of the tests in this module have the output which
   is unspecified because it depends on the implementain details of [Async].
   It is unlikely that the implementation of [Async] will change significantly
   in the future, but if it does we should look into rewriting the tests to only
   check conditions which should hold no matter the implemenation. *)
let without_backtraces f =
  let elide_backtraces = !Backtrace.elide in
  Backtrace.elide := true;
  Monitor.protect f ~finally:(fun () ->
    Backtrace.elide := elide_backtraces;
    return ())
;;

let sequence ?(stop = 10) () = Sequence.range 0 stop

let sequence' ?(stop = 20) ?(queue_length = 2) () =
  sequence ~stop ()
  |> Fn.flip Sequence.chunks_exn queue_length
  |> Sequence.map ~f:Queue.of_list
;;

let print ?label i =
  print_s [%message.omit_nil "" ~_:(label : string option) ~_:(i : int)]
;;

let print' ?label is =
  print_s [%message.omit_nil "" ~_:(label : string option) ~_:(is : int Queue.t)]
;;

let consumer = Iterator.create_consumer ~f:(print >> Maybe_pushback.return)
let consumer' = Iterator.create_consumer ~f:(print' >> Maybe_pushback.return)
let print_stop_reason reason = print_s [%sexp (reason : unit Or_error.t)]

type args =
  { batch_size : int
  ; stop : int option
  }
[@@deriving bin_io]

let start_rpc = Rpc_iterator.start_rpc ()
let iter_rpc = Rpc_iterator.iter_rpc ~bin_args ~bin_message:Int.bin_t ()
let stopped_rpc = Rpc_iterator.stopped_rpc ()

let client ~create_producer ~create_consumer () =
  Rpc_transport.client
    ~server_implementations:
      (Rpc.Implementations.create_exn
         ~implementations:
           [ Rpc.One_way.implement
               start_rpc
               Rpc_iterator.implement_start
               ~on_exception:Close_connection
           ; Rpc.Pipe_rpc.implement_direct
               iter_rpc
               (Staged.unstage
                  (Rpc_iterator.implement_iter ~create_producer ~create_consumer))
           ; Rpc.Rpc.implement stopped_rpc Rpc_iterator.implement_stopped
           ]
         ~on_unknown_rpc:`Raise
         ~on_exception:Log_on_background_exn)
    ~server_connection_state:(fun (_ : Rpc.Connection.t) ->
      Rpc_iterator.Connection_state.create ())
;;

let iter_pipe_rpc
  ?(batch_size = 2)
  ?start
  ?stop
  ?(remote_producer =
    fun { batch_size = _; stop } -> return (Ok (Iterator.of_sequence (sequence ?stop ()))))
  ?(remote_consumer =
    fun { batch_size; stop = _ } writer ->
      Iterator.of_direct_stream_writer ~flush_every:batch_size writer
      |> Iterator.contra_inspect ~f:(print ~label:"direct_stream_writer")
      |> Ok
      |> return)
  consumer
  =
  let%bind.Deferred.Or_error producer =
    Rpc_iterator.Global.of_pipe_rpc
      (client ~create_producer:remote_producer ~create_consumer:remote_consumer)
      { batch_size; stop }
      ~start_rpc
      ~iter_rpc
      ~stopped_rpc
    >>| Or_error.tag ~tag:"iter failed completely"
  in
  let producer =
    match start with
    | Some start -> Iterator.add_start producer ~start
    | None -> producer
  in
  Iterator.start producer consumer
;;

let iter_pipe_rpc'
  ?(batch_size = 2)
  ?stop
  ?(remote_producer =
    fun { batch_size; stop } ->
      return (Ok (Iterator.of_sequence (sequence' ?stop ~queue_length:batch_size ()))))
  ?(remote_consumer =
    fun _ writer ->
      Iterator.Batched.of_direct_stream_writer writer
      |> Iterator.contra_inspect ~f:(print' ~label:"direct_stream_writer")
      |> Ok
      |> return)
  consumer
  =
  let%bind.Deferred.Or_error producer =
    Rpc_iterator.Global.of_pipe_rpc
      (client ~create_producer:remote_producer ~create_consumer:remote_consumer)
      { batch_size; stop }
      ~start_rpc
      ~iter_rpc
      ~stopped_rpc
    >>| Or_error.tag ~tag:"iter failed completely"
  in
  Iterator.start producer consumer
;;

let%expect_test "iter_pipe_rpc respects pushback" =
  let%bind () =
    Pipe.create_writer ~size_budget:0 (fun reader ->
      Iterator.start (Iterator.of_pipe_reader reader) (consumer ()) >>| ok_exn)
    |> Iterator.of_pipe_writer
    |> Iterator.contra_inspect ~f:(print ~label:"pipe_writer")
    |> iter_pipe_rpc ~batch_size:2
    >>| ok_exn
  in
  [%expect
    {|
    (direct_stream_writer 0)
    (direct_stream_writer 1)
    (pipe_writer 0)
    (direct_stream_writer 2)
    (direct_stream_writer 3)
    0
    (pipe_writer 1)
    1
    (pipe_writer 2)
    (direct_stream_writer 4)
    (direct_stream_writer 5)
    2
    (pipe_writer 3)
    3
    (pipe_writer 4)
    (direct_stream_writer 6)
    (direct_stream_writer 7)
    4
    (pipe_writer 5)
    5
    (pipe_writer 6)
    (direct_stream_writer 8)
    (direct_stream_writer 9)
    6
    (pipe_writer 7)
    7
    (pipe_writer 8)
    8
    (pipe_writer 9)
    9
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc' respects pushback" =
  let%bind () =
    Pipe.create_writer ~size_budget:1 (fun reader ->
      Iterator.start (Iterator.Batched.of_pipe_reader reader) (consumer' ()) >>| ok_exn)
    |> Iterator.of_pipe_writer
    |> Iterator.contra_inspect ~f:(print ~label:"pipe_writer")
    |> iter_pipe_rpc' ~batch_size:4
    >>| ok_exn
  in
  [%expect
    {|
    (direct_stream_writer (0 1 2 3))
    (pipe_writer 0)
    (pipe_writer 1)
    (direct_stream_writer (4 5 6 7))
    (0 1)
    (pipe_writer 2)
    (pipe_writer 3)
    (2 3)
    (pipe_writer 4)
    (pipe_writer 5)
    (direct_stream_writer (8 9 10 11))
    (4 5)
    (pipe_writer 6)
    (pipe_writer 7)
    (6 7)
    (pipe_writer 8)
    (pipe_writer 9)
    (direct_stream_writer (12 13 14 15))
    (8 9)
    (pipe_writer 10)
    (pipe_writer 11)
    (10 11)
    (pipe_writer 12)
    (pipe_writer 13)
    (direct_stream_writer (16 17 18 19))
    (12 13)
    (pipe_writer 14)
    (pipe_writer 15)
    (14 15)
    (pipe_writer 16)
    (pipe_writer 17)
    (16 17)
    (pipe_writer 18)
    (pipe_writer 19)
    (18 19)
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc works with an empty stream" =
  let%bind () = consumer () |> iter_pipe_rpc ~stop:0 >>| ok_exn in
  [%expect {| |}];
  return ()
;;

(* It's not really interesting to test all of the [iter_pipe_rpc] behavior for both
   [of_direct_stream_writer] and [of_direct_stream_writer'], since the only real
   difference is when exactly they check for pushback, and the above two tests demonstrate
   that they both work as expected. *)

let%expect_test "iter_pipe_rpc respects start" =
  let start = Ivar.create () in
  let stopped = consumer () |> iter_pipe_rpc ~start:(Ivar.read start) >>| ok_exn in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  Ivar.fill_exn start ();
  let%bind () = stopped in
  [%expect
    {|
    (direct_stream_writer 0)
    (direct_stream_writer 1)
    0
    1
    (direct_stream_writer 2)
    (direct_stream_writer 3)
    2
    3
    (direct_stream_writer 4)
    (direct_stream_writer 5)
    4
    5
    (direct_stream_writer 6)
    (direct_stream_writer 7)
    6
    7
    (direct_stream_writer 8)
    (direct_stream_writer 9)
    8
    9
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc respects stop" =
  let stop = Ivar.create () in
  let%bind () =
    consumer ~stop:(Ivar.read stop) ()
    |> Iterator.contra_inspect ~f:(function
      | 4 -> Ivar.fill_exn stop ()
      | _ -> ())
    |> iter_pipe_rpc ~stop:20
    >>| ok_exn
  in
  [%expect
    {|
    (direct_stream_writer 0)
    (direct_stream_writer 1)
    0
    1
    (direct_stream_writer 2)
    (direct_stream_writer 3)
    2
    3
    (direct_stream_writer 4)
    (direct_stream_writer 5)
    (direct_stream_writer 6)
    (direct_stream_writer 7)
    (direct_stream_writer 8)
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc respects add_stop" =
  let stop = Ivar.create () in
  let%bind () =
    consumer ()
    |> Iterator.contra_inspect ~f:(function
      | 4 -> Ivar.fill_exn stop ()
      | _ -> ())
    |> Iterator.add_stop ~stop:(Ivar.read stop)
    |> iter_pipe_rpc ~stop:20
    >>| ok_exn
  in
  [%expect
    {|
    (direct_stream_writer 0)
    (direct_stream_writer 1)
    0
    1
    (direct_stream_writer 2)
    (direct_stream_writer 3)
    2
    3
    (direct_stream_writer 4)
    (direct_stream_writer 5)
    4
    (direct_stream_writer 6)
    (direct_stream_writer 7)
    (direct_stream_writer 8)
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc stops even if not started" =
  let stop = Ivar.create () in
  let stopped =
    consumer ~stop:(Ivar.read stop) () |> iter_pipe_rpc ~start:(never ()) >>| ok_exn
  in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  Ivar.fill_exn stop ();
  let%bind () = stopped in
  [%expect {| (direct_stream_writer 0) |}];
  return ()
;;

let%expect_test "iter_pipe_rpc fails if remote implementation fails" =
  let%bind () =
    consumer ()
    |> iter_pipe_rpc ~remote_producer:(fun _ -> return (Error (Error.of_string "oops")))
    >>| print_stop_reason
  in
  [%expect {| (Error ("iter failed completely" oops)) |}];
  return ()
;;

let%expect_test "iter_pipe_rpc stops on local error" =
  let%bind () =
    without_backtraces (fun () ->
      consumer ()
      |> Iterator.contra_inspect ~f:(function
        | 4 -> failwith "4"
        | _ -> ())
      |> iter_pipe_rpc
      >>| print_stop_reason)
  in
  [%expect
    {|
    (direct_stream_writer 0)
    (direct_stream_writer 1)
    0
    1
    (direct_stream_writer 2)
    (direct_stream_writer 3)
    2
    3
    (direct_stream_writer 4)
    (direct_stream_writer 5)
    (direct_stream_writer 6)
    (direct_stream_writer 7)
    (direct_stream_writer 8)
    (Error
     (("pipe rpc closed"
       (Uncaught_exn
        (monitor.ml.Error (Failure 4)
         ("<backtrace elided in test>" "Caught by monitor RPC connection loop"))))
      ("unknown remote stop reason"
       ((rpc_error
         (Uncaught_exn
          (monitor.ml.Error (Failure 4)
           ("<backtrace elided in test>" "Caught by monitor RPC connection loop"))))
        (connection_description <created-directly>) (rpc_name stopped)
        (rpc_version 0)))))
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc stops on remote error" =
  let%bind () =
    without_backtraces (fun () ->
      consumer ()
      |> iter_pipe_rpc' ~remote_consumer:(fun _ writer ->
        Iterator.Batched.of_direct_stream_writer writer
        |> Iterator.Batched.contra_inspect ~f:(function
          | 4 -> failwith "4"
          | _ -> ())
        |> Ok
        |> return)
      >>| print_stop_reason)
  in
  [%expect
    {|
    0
    1
    2
    3
    (Error
     ("remote stopped"
      (monitor.ml.Error (Failure 4)
       ("<backtrace elided in test>" "Caught by monitor try_with_join_or_error"))))
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc fails to start if connection is closed" =
  let%bind connection =
    client
      ~create_producer:(fun { batch_size = _; stop } ->
        return (Ok (Iterator.of_sequence (sequence ?stop ()))))
      ~create_consumer:(fun { batch_size; stop = _ } writer ->
        return (Ok (Iterator.of_direct_stream_writer ~flush_every:batch_size writer)))
      ()
    >>| ok_exn
  in
  let%bind producer =
    Rpc_iterator.Global.of_pipe_rpc
      (fun () -> return (Ok connection))
      { batch_size = 2; stop = None }
      ~start_rpc
      ~iter_rpc
      ~stopped_rpc
    >>| ok_exn
  in
  let%bind () = Rpc.Connection.close connection in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  [%expect {| |}];
  let%bind () = Iterator.start producer (consumer ()) >>| print_stop_reason in
  [%expect
    {|
    (Error
     (("failed to start"
       ((rpc_error (Connection_closed (Rpc.Connection.close)))
        (connection_description <created-directly>) (rpc_name start)
        (rpc_version 0)))
      ("pipe rpc closed" (Connection_closed (Rpc.Connection.close)))
      ("unknown remote stop reason"
       ((rpc_error (Connection_closed (Rpc.Connection.close)))
        (connection_description <created-directly>) (rpc_name stopped)
        (rpc_version 0)))))
    |}];
  return ()
;;

let%expect_test "iter_pipe_rpc stops if connection is closed" =
  let%bind connection =
    client
      ~create_producer:(fun { batch_size = _; stop } ->
        return (Ok (Iterator.of_sequence (sequence ?stop ()))))
      ~create_consumer:(fun { batch_size; stop = _ } writer ->
        Iterator.of_direct_stream_writer ~flush_every:batch_size writer
        |> Iterator.contra_inspect ~f:(print ~label:"direct_stream_writer")
        |> Ok
        |> return)
      ()
    >>| ok_exn
  in
  let%bind producer =
    Rpc_iterator.Global.of_pipe_rpc
      (fun () -> return (Ok connection))
      { batch_size = 2; stop = None }
      ~start_rpc
      ~iter_rpc
      ~stopped_rpc
    >>| ok_exn
  in
  let saw_at_least_one_message = Ivar.create () in
  let consumer =
    consumer ()
    |> Iterator.contra_inspect ~f:(fun _ ->
      Ivar.fill_if_empty saw_at_least_one_message ())
  in
  let stopped = Iterator.start producer consumer in
  let%bind () = Ivar.read saw_at_least_one_message in
  let%bind () = Rpc.Connection.close connection in
  let%bind () = stopped >>| print_stop_reason in
  [%expect
    {|
    (direct_stream_writer 0)
    (direct_stream_writer 1)
    0
    1
    (direct_stream_writer 2)
    (direct_stream_writer 3)
    2
    3
    (direct_stream_writer 4)
    (direct_stream_writer 5)
    (direct_stream_writer 6)
    (direct_stream_writer 7)
    (direct_stream_writer 8)
    (direct_stream_writer 9)
    (Error
     (("pipe rpc closed" (Connection_closed (Rpc.Connection.close)))
      ("unknown remote stop reason"
       ((rpc_error (Connection_closed (Rpc.Connection.close)))
        (connection_description <created-directly>) (rpc_name stopped)
        (rpc_version 0)))))
    |}];
  return ()
;;

let%expect_test "[create_producer] produces error" =
  let writer_closed = Ivar.create () in
  let%bind connection =
    client
      ~create_producer:(fun (_ : args) -> Deferred.Or_error.error_string "producer error")
      ~create_consumer:(fun { batch_size; stop = _ } writer ->
        upon
          (Rpc.Pipe_rpc.Direct_stream_writer.closed writer)
          (Ivar.fill_exn writer_closed);
        Iterator.of_direct_stream_writer ~flush_every:batch_size writer
        |> Iterator.contra_inspect ~f:(fun (_ : int) -> failwith "should be unreachable")
        |> Ok
        |> return)
      ()
    >>| ok_exn
  in
  let%bind producer =
    Rpc_iterator.Global.of_pipe_rpc
      (fun () -> return (Ok connection))
      { batch_size = 1; stop = None }
      ~start_rpc
      ~iter_rpc
      ~stopped_rpc
  in
  print_s [%sexp (producer : _ Or_error.t)];
  [%expect {| (Error "producer error") |}];
  Ivar.read writer_closed
;;

let%expect_test "[create_consumer] produces error" =
  let writer_closed = Ivar.create () in
  let%bind connection =
    client
      ~create_producer:(fun { batch_size = _; stop } ->
        return (Ok (Iterator.of_sequence (sequence ?stop ()))))
      ~create_consumer:(fun (_ : args) writer ->
        upon
          (Rpc.Pipe_rpc.Direct_stream_writer.closed writer)
          (Ivar.fill_exn writer_closed);
        Deferred.Or_error.error_string "consumer error")
      ()
    >>| ok_exn
  in
  let%bind producer =
    Rpc_iterator.Global.of_pipe_rpc
      (fun () -> return (Ok connection))
      { batch_size = 1; stop = None }
      ~start_rpc
      ~iter_rpc
      ~stopped_rpc
  in
  print_s [%sexp (producer : _ Or_error.t)];
  [%expect {| (Error "consumer error") |}];
  Ivar.read writer_closed
;;
