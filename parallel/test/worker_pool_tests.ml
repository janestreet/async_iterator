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

(* 3 because 1 is degenerate and some async things are weird with only 2, e.g. which order
   ivar handlers run in, an optimization for [choose], etc. *)
let number_of_workers = 3

let iter_with
  ?(create_producer = create_producer)
  ?(bin_message = Message.bin_t)
  ?(info = fun (_ : int) -> Info.create ())
  consumer
  =
  let worker =
    Worker.make
      ~create_producer
      ~create_consumer:(fun _ writer ->
        return (Ok (Iterator.Batched.of_direct_stream_writer writer)))
      ~bin_args:Args.bin_t
      ~bin_message
  in
  let workers =
    List.init number_of_workers ~f:(fun worker -> (), info worker)
    |> Nonempty_list.of_list_exn
  in
  match%bind
    Iterator.create_producer_with_resource
      (fun () -> Worker_pool.create worker workers)
      ~close:(Worker_pool.close >> Deferred.ok)
      ~create:
        (Worker_pool.create_producer ~args:(fun ~worker () info -> { Args.info; worker }))
  with
  | Error error ->
    print_s [%message "Failed to start" (error : Error.t)];
    return ()
  | Ok producer ->
    let%bind reason = Iterator.start producer consumer in
    print_s [%message "Stopped" (reason : unit Or_error.t)];
    return ()
;;

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
