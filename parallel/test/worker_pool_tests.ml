open! Core
open! Async
open! Import
open Test_helpers

let%expect_test "default behavior" =
  let%bind () = iter_with (consumer ()) in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    ((worker 1) (i 2))
    ((worker 2) (i 2))
    ((worker 0) (i 3))
    ((worker 1) (i 3))
    ((worker 2) (i 3))
    ((worker 0) (i 4))
    ((worker 1) (i 4))
    ((worker 2) (i 4))
    (Stopped (reason (Ok ())))
    |}];
  return ()
;;

let%expect_test "respects size budget" =
  let%bind () =
    iter_with ~info:(fun (_ : int) -> Info.create ~size_budget:1 ()) (consumer ())
  in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 0))
    ((worker 1) (i 1))
    ((worker 2) (i 0))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    ((worker 0) (i 3))
    ((worker 1) (i 2))
    ((worker 1) (i 3))
    ((worker 2) (i 2))
    ((worker 2) (i 3))
    ((worker 0) (i 4))
    ((worker 1) (i 4))
    ((worker 2) (i 4))
    (Stopped (reason (Ok ())))
    |}];
  return ()
;;

let%expect_test "respects stop" =
  let%bind (_ : unit list) =
    Deferred.List.init ~how:`Sequential number_of_workers ~f:(fun worker ->
      let%bind () = iter_with (consumer ~stop_after:{ worker; i = 2 } ()) in
      print_newline ();
      return ())
  in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    (Stopped (reason (Ok ())))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    ((worker 1) (i 2))
    (Stopped (reason (Ok ())))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    ((worker 1) (i 2))
    ((worker 2) (i 2))
    (Stopped (reason (Ok ())))
    |}];
  return ()
;;

let%expect_test "iteration delayed until all workers start successfully" =
  let%bind (_ : unit list) =
    Deferred.List.init ~how:`Sequential number_of_workers ~f:(fun failing_worker ->
      iter_with
        ~create_producer:(fun args ->
          if args.worker = failing_worker
          then return (Error (Error.of_string "FAKE ERROR"))
          else create_producer args)
        (consumer ()))
  in
  [%expect
    {|
    ("Failed to start" (error (worker 0 "FAKE ERROR")))
    ("Failed to start" (error (worker 1 "FAKE ERROR")))
    ("Failed to start" (error (worker 2 "FAKE ERROR")))
    |}];
  return ()
;;

(* Note that other workers keep going for a few messages after the first one fails. This
   is acceptable because output interleaving is inherently racy. *)
let%expect_test "iteration stops when any worker stops due to error" =
  let%bind (_ : unit list) =
    Deferred.List.init ~how:`Sequential number_of_workers ~f:(fun failing_worker ->
      iter_with
        ~bin_message:
          { Message.bin_t with
            reader =
              Bin_prot.Type_class.cnv_reader
                (fun message ->
                  if Message.equal message { worker = failing_worker; i = 2 }
                  then (
                    printf "WORKER %d FAILS HERE\n" failing_worker;
                    failwith "BAD INT")
                  else message)
                Message.bin_reader_t
          }
        (consumer ())
      >>| print_newline)
  in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    WORKER 0 FAILS HERE
    ((worker 1) (i 2))
    ((worker 2) (i 2))
    ((worker 1) (i 3))
    ((worker 2) (i 3))
    ((worker 1) (i 4))
    ((worker 2) (i 4))
    (Stopped
     (reason
      (Error
       (worker 0
        ("pipe rpc closed"
         (Bin_io_exn
          ((location
            "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
           (exn (Failure "BAD INT")))))))))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    WORKER 1 FAILS HERE
    ((worker 2) (i 2))
    ((worker 0) (i 3))
    ((worker 2) (i 3))
    ((worker 0) (i 4))
    ((worker 2) (i 4))
    (Stopped
     (reason
      (Error
       (worker 1
        ("pipe rpc closed"
         (Bin_io_exn
          ((location
            "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
           (exn (Failure "BAD INT")))))))))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    ((worker 1) (i 2))
    WORKER 2 FAILS HERE
    ((worker 0) (i 3))
    ((worker 1) (i 3))
    ((worker 0) (i 4))
    ((worker 1) (i 4))
    (Stopped
     (reason
      (Error
       (worker 2
        ("pipe rpc closed"
         (Bin_io_exn
          ((location
            "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
           (exn (Failure "BAD INT")))))))))
    |}];
  return ()
;;

let%expect_test "multiple workers stop due to error" =
  let%bind (_ : unit list) =
    (* [number_of_workers + 1] means we have one iteration where all fail. *)
    Deferred.List.init
      ~how:`Sequential
      (number_of_workers + 1)
      ~f:(fun succeeding_worker ->
        iter_with
          ~bin_message:
            { Message.bin_t with
              reader =
                Bin_prot.Type_class.cnv_reader
                  (fun (message : Message.t) ->
                    if message.worker = succeeding_worker || message.i < 2
                    then message
                    else (
                      printf "WORKER %d FAILS HERE\n" message.worker;
                      failwith "BAD INT"))
                  Message.bin_reader_t
            }
          (consumer ())
        >>| print_newline)
  in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    WORKER 1 FAILS HERE
    WORKER 2 FAILS HERE
    ((worker 0) (i 3))
    ((worker 0) (i 4))
    (Stopped
     (reason
      (Error
       ((worker 1
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))
        (worker 2
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))))))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    WORKER 0 FAILS HERE
    ((worker 1) (i 2))
    WORKER 2 FAILS HERE
    ((worker 1) (i 3))
    ((worker 1) (i 4))
    (Stopped
     (reason
      (Error
       ((worker 0
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))
        (worker 2
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))))))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    WORKER 0 FAILS HERE
    WORKER 1 FAILS HERE
    ((worker 2) (i 2))
    ((worker 2) (i 3))
    ((worker 2) (i 4))
    (Stopped
     (reason
      (Error
       ((worker 0
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))
        (worker 1
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))))))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    WORKER 0 FAILS HERE
    WORKER 1 FAILS HERE
    WORKER 2 FAILS HERE
    (Stopped
     (reason
      (Error
       ((worker 0
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))
        (worker 1
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))
        (worker 2
         ("pipe rpc closed"
          (Bin_io_exn
           ((location
             "client-side streaming_rpc response un-bin-io'ing to [bin_reader_update]")
            (exn (Failure "BAD INT"))))))))))
    |}];
  return ()
;;

let%expect_test "iteration waits for all workers to finish" =
  let%bind () =
    iter_with ~info:(fun worker -> Info.create ~stop:(worker + 1) ()) (consumer ())
  in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 2) (i 2))
    (Stopped (reason (Ok ())))
    |}];
  return ()
;;

let%expect_test "pushback is effectively per-worker" =
  let%bind (_ : unit list) =
    Deferred.List.init ~how:`Sequential number_of_workers ~f:(fun slow_worker ->
      let pushback = Ivar.create () in
      let stopped =
        iter_with
          ~info:(fun (_ : int) -> Info.create ())
          (consumer
             ~pushback:(fun { worker; i = _ } ->
               if worker = slow_worker
               then Ivar.read pushback |> Maybe_pushback.of_deferred
               else Maybe_pushback.unit)
             ())
      in
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      Ivar.fill_exn pushback ();
      let%bind () = stopped in
      print_newline ();
      return ())
  in
  [%expect
    {|
    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 1) (i 1))
    ((worker 2) (i 1))
    ((worker 1) (i 2))
    ((worker 2) (i 2))
    ((worker 1) (i 3))
    ((worker 2) (i 3))
    ((worker 1) (i 4))
    ((worker 2) (i 4))
    ((worker 0) (i 1))
    ((worker 0) (i 2))
    ((worker 0) (i 3))
    ((worker 0) (i 4))
    (Stopped (reason (Ok ())))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 2) (i 1))
    ((worker 0) (i 2))
    ((worker 2) (i 2))
    ((worker 0) (i 3))
    ((worker 2) (i 3))
    ((worker 0) (i 4))
    ((worker 2) (i 4))
    ((worker 1) (i 1))
    ((worker 1) (i 2))
    ((worker 1) (i 3))
    ((worker 1) (i 4))
    (Stopped (reason (Ok ())))

    ((worker 0) (i 0))
    ((worker 1) (i 0))
    ((worker 2) (i 0))
    ((worker 0) (i 1))
    ((worker 1) (i 1))
    ((worker 0) (i 2))
    ((worker 1) (i 2))
    ((worker 0) (i 3))
    ((worker 1) (i 3))
    ((worker 0) (i 4))
    ((worker 1) (i 4))
    ((worker 2) (i 1))
    ((worker 2) (i 2))
    ((worker 2) (i 3))
    ((worker 2) (i 4))
    (Stopped (reason (Ok ())))
    |}];
  return ()
;;
