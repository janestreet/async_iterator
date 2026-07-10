open! Core
open! Async
open! Import
open Test_helpers

let%expect_test "update_worker_state updates state in the specified worker" =
  with_worker_pool (make_state_worker ()) (default_workers ~stop:3 ()) ~f:(fun pool ->
    let%bind producer = create_producer_from_worker_pool pool in
    let%bind () = Worker_pool.update_worker_state pool ~worker:1 100 >>| ok_exn in
    let%bind reason = Iterator.start producer (consumer ()) in
    print_s [%message "Stopped" (reason : unit Or_error.t)];
    [%expect
      {|
      ((worker 0) (i 0))
      ((worker 1) (i 100))
      ((worker 2) (i 0))
      ((worker 0) (i 1))
      ((worker 1) (i 101))
      ((worker 2) (i 1))
      ((worker 0) (i 2))
      ((worker 1) (i 102))
      ((worker 2) (i 2))
      (Stopped (reason (Ok ())))
      |}];
    return ())
;;

let%expect_test "update_worker_state returns an error for out of bounds worker index" =
  with_worker_pool (make_state_worker ()) (default_workers ()) ~f:(fun pool ->
    let%bind result = Worker_pool.update_worker_state pool ~worker:10 0 in
    print_s [%sexp (result : unit Or_error.t)];
    [%expect
      {| (Error ("Worker index out of bounds" (worker 10) (number_of_workers 3))) |}];
    return ())
;;

let%expect_test "worker state is reinitialized for each iteration" =
  let workers = Nonempty_list.singleton ((), Info.create ~stop:2 ()) in
  let init (_ : Args.t) =
    print_s [%message "Init called"];
    return (Ok { Worker_state.offset = 0 })
  in
  with_worker_pool (make_state_worker ~init ()) workers ~f:(fun pool ->
    (* First iteration - state.offset starts at 0 (from init), then gets set to 100. *)
    let%bind producer1 = create_producer_from_worker_pool pool in
    let%bind () = Worker_pool.update_worker_state pool ~worker:0 100 >>| ok_exn in
    let%bind reason1 = Iterator.start producer1 (consumer ()) in
    print_s [%message "Stopped" (reason1 : unit Or_error.t)];
    [%expect
      {|
      "Init called"
      ((worker 0) (i 100))
      ((worker 0) (i 101))
      (Stopped (reason1 (Ok ())))
      |}];
    (* Second iteration - since init creates fresh state, offset should be 0 again. *)
    let%bind producer2 = create_producer_from_worker_pool pool in
    let%bind reason2 = Iterator.start producer2 (consumer ()) in
    print_s [%message "Stopped" (reason2 : unit Or_error.t)];
    [%expect
      {|
      "Init called"
      ((worker 0) (i 0))
      ((worker 0) (i 1))
      (Stopped (reason2 (Ok ())))
      |}];
    return ())
;;

let%expect_test "update_worker_state returns an error when iteration hasn't started" =
  with_worker_pool (make_state_worker ()) (default_workers ()) ~f:(fun pool ->
    let%bind result = Worker_pool.update_worker_state pool ~worker:0 0 in
    print_s [%sexp (result : unit Or_error.t)];
    [%expect
      {|
      (Error
       "Cannot update worker state: state not initialized (worker has not started processing yet)")
      |}];
    return ())
;;

let%expect_test "update_worker_state returns an error after an iteration finishes" =
  let workers = Nonempty_list.singleton ((), Info.create ~stop:2 ()) in
  with_worker_pool (make_state_worker ()) workers ~f:(fun pool ->
    (* Run first iteration to completion *)
    let%bind producer = create_producer_from_worker_pool pool in
    let%bind reason = Iterator.start producer (consumer ()) in
    print_s [%message "Stopped" (reason : unit Or_error.t)];
    [%expect
      {|
      ((worker 0) (i 0))
      ((worker 0) (i 1))
      (Stopped (reason (Ok ())))
      |}];
    (* Try to update state between iterations - should fail since state was cleared *)
    let%bind result = Worker_pool.update_worker_state pool ~worker:0 100 in
    print_s [%sexp (result : unit Or_error.t)];
    [%expect
      {|
      (Error
       "Cannot update worker state: state not initialized (worker has not started processing yet)")
      |}];
    return ())
;;

let%expect_test "update_worker_state mid-iteration affects subsequent messages" =
  let pipe_reader, pipe_writer = Pipe.create () in
  let workers = Nonempty_list.singleton ((), Info.create ()) in
  with_worker_pool
    (make_state_worker
       ~create_producer:(fun (_ : Args.t) ->
         return (Ok (Iterator.Batched.of_pipe_reader pipe_reader)))
       ())
    workers
    ~f:(fun pool ->
      let%bind producer = create_producer_from_worker_pool pool in
      let iteration = Iterator.start producer (consumer ()) in
      (* Write first message before any state update *)
      let%bind () = Pipe.write pipe_writer { worker = 0; i = 1 } in
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| ((worker 0) (i 1)) |}];
      (* Update state mid-iteration *)
      let%bind () = Worker_pool.update_worker_state pool ~worker:0 100 >>| ok_exn in
      (* Write second message - should reflect the updated offset *)
      let%bind () = Pipe.write pipe_writer { worker = 0; i = 2 } in
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| ((worker 0) (i 102)) |}];
      (* Close the pipe to end the iteration *)
      Pipe.close pipe_writer;
      let%bind reason = iteration in
      print_s [%message "Stopped" (reason : unit Or_error.t)];
      [%expect {| (Stopped (reason (Ok ()))) |}];
      return ())
;;

let%expect_test "update_worker_state propagates callback errors" =
  let pipe_reader, pipe_writer = Pipe.create () in
  let failing_worker =
    Worker.make
      ~init:(fun (_ : Args.t) -> return (Ok ()))
      ~create_producer:(fun (_ : Args.t) ->
        return (Ok (Iterator.Batched.of_pipe_reader pipe_reader)))
      ~create_consumer:(fun () (_ : Args.t) writer ->
        return (Ok (Iterator.Batched.of_direct_stream_writer writer)))
      ~state_updating:
        (Using
           { update_worker_state =
               (fun () () ->
                 return (Or_error.error_string "intentional callback failure"))
           ; bin_state_update = bin_unit
           })
      ~bin_args:Args.bin_t
      ~bin_message:Message.bin_t
  in
  let workers = Nonempty_list.singleton ((), Info.create ()) in
  with_worker_pool failing_worker workers ~f:(fun pool ->
    let%bind producer = create_producer_from_worker_pool pool in
    let iteration = Iterator.start producer (consumer ()) in
    (* Write a message to confirm iteration is running *)
    let%bind () = Pipe.write pipe_writer { Message.worker = 0; i = 0 } in
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    [%expect {| ((worker 0) (i 0)) |}];
    (* Now update_worker_state should reach the callback and get its error *)
    let%bind result = Worker_pool.update_worker_state pool ~worker:0 () in
    print_s [%sexp (result : unit Or_error.t)];
    [%expect {| (Error "intentional callback failure") |}];
    Pipe.close pipe_writer;
    let%bind reason = iteration in
    print_s [%message "Stopped" (reason : unit Or_error.t)];
    [%expect {| (Stopped (reason (Ok ()))) |}];
    return ())
;;
