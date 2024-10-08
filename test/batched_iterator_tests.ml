open! Core
open! Async
open! Import

(* Similar to [Global_iterator_tests], these tests do not exhaustively check the various
   [create] functions. However, batched operations are meaningfully different, and we
   cover them more thoroughly below. *)
module%test [@name "basic operations"] _ = struct
  open Shared_setup.Batched

  let run_operation_test_for_all_producers_and_consumers ~operation ~expect =
    let run_operation_test_for_all_producers ~create_consumer =
      run_test_for_all_producers
        ~iter:(fun ~f ~stop:_ ->
          for x = 0 to 4 do
            let xs = Queue.of_list [ x; x + 5 ] in
            print_s [%sexp "producer", (xs : int Queue.t)];
            let pushback = f xs in
            assert (Deferred.is_determined (Maybe_pushback.to_deferred pushback));
            print_newline ()
          done;
          expect ();
          return (Ok ()))
        ~iter':(fun ~f ~stop:_ ->
          for x = 0 to 4 do
            let xs = Queue.of_list [ x; x + 5 ] in
            print_s [%sexp "producer", (xs : int Queue.t)];
            let action = f xs in
            (match action with
             | Continue -> ()
             | Stop | Wait { pushback = _ } -> assert false);
            print_newline ()
          done;
          expect ();
          return (Ok ()))
        ~operation
        ~create_consumer
    in
    let%bind () =
      run_operation_test_for_all_producers ~create_consumer:(fun () ->
        Iterator.create_consumer
          ~f:(fun xs ->
            print_s [%sexp "consumer", (xs : int Queue.t)];
            Maybe_pushback.unit)
          ())
    in
    let%bind () =
      run_operation_test_for_all_producers ~create_consumer:(fun () ->
        Iterator.create_consumer'
          ~f:(fun xs ->
            print_s [%sexp "consumer", (xs : int Queue.t)];
            Continue)
          ())
    in
    return ()
  ;;

  let%expect_test "no operation" =
    run_operation_test_for_all_producers_and_consumers ~operation:Ident ~expect:(fun () ->
      [%expect
        {|
        (producer (0 5))
        (consumer (0 5))

        (producer (1 6))
        (consumer (1 6))

        (producer (2 7))
        (consumer (2 7))

        (producer (3 8))
        (consumer (3 8))

        (producer (4 9))
        (consumer (4 9))
        |}])
  ;;

  let%expect_test "[inspect] and [contra_inspect]" =
    run_operation_test_for_all_producers_and_consumers
      ~operation:(Inspect (fun x -> print_s [%sexp "inspect", (x : int)]))
      ~expect:(fun () ->
        [%expect
          {|
          (producer (0 5))
          (inspect 0)
          (inspect 5)
          (consumer (0 5))

          (producer (1 6))
          (inspect 1)
          (inspect 6)
          (consumer (1 6))

          (producer (2 7))
          (inspect 2)
          (inspect 7)
          (consumer (2 7))

          (producer (3 8))
          (inspect 3)
          (inspect 8)
          (consumer (3 8))

          (producer (4 9))
          (inspect 4)
          (inspect 9)
          (consumer (4 9))
          |}])
  ;;

  let%expect_test "[filter] and [contra_filter]" =
    run_operation_test_for_all_producers_and_consumers
      ~operation:
        (Filter
           (fun x ->
             let is_even = x % 2 = 0 in
             print_s [%sexp "filter", (x : int), (is_even : bool)];
             is_even))
      ~expect:(fun () ->
        [%expect
          {|
          (producer (0 5))
          (filter 0 true)
          (filter 5 false)
          (consumer (0))

          (producer (1 6))
          (filter 1 false)
          (filter 6 true)
          (consumer (6))

          (producer (2 7))
          (filter 2 true)
          (filter 7 false)
          (consumer (2))

          (producer (3 8))
          (filter 3 false)
          (filter 8 true)
          (consumer (8))

          (producer (4 9))
          (filter 4 true)
          (filter 9 false)
          (consumer (4))
          |}])
  ;;

  let%expect_test "[map] and [contra_map]" =
    run_operation_test_for_all_producers_and_consumers
      ~operation:
        (Map
           (fun x ->
             let negative_x = -x in
             print_s [%sexp "map", (x : int), (negative_x : int)];
             negative_x))
      ~expect:(fun () ->
        [%expect
          {|
          (producer (0 5))
          (map 0 0)
          (map 5 -5)
          (consumer (0 -5))

          (producer (1 6))
          (map 1 -1)
          (map 6 -6)
          (consumer (-1 -6))

          (producer (2 7))
          (map 2 -2)
          (map 7 -7)
          (consumer (-2 -7))

          (producer (3 8))
          (map 3 -3)
          (map 8 -8)
          (consumer (-3 -8))

          (producer (4 9))
          (map 4 -4)
          (map 9 -9)
          (consumer (-4 -9))
          |}])
  ;;

  let%expect_test "[filter_map] and [contra_filter_map]" =
    run_operation_test_for_all_producers_and_consumers
      ~operation:
        (Filter_map
           (fun x ->
             let negative_x = if x % 2 = 0 then Some (-x) else None in
             print_s
               [%sexp "filter_map", (x : int), (negative_x : (int option[@sexp.option]))];
             negative_x))
      ~expect:(fun () ->
        [%expect
          {|
          (producer (0 5))
          (filter_map 0 0)
          (filter_map 5)
          (consumer (0))

          (producer (1 6))
          (filter_map 1)
          (filter_map 6 -6)
          (consumer (-6))

          (producer (2 7))
          (filter_map 2 -2)
          (filter_map 7)
          (consumer (-2))

          (producer (3 8))
          (filter_map 3)
          (filter_map 8 -8)
          (consumer (-8))

          (producer (4 9))
          (filter_map 4 -4)
          (filter_map 9)
          (consumer (-4))
          |}])
  ;;

  let%expect_test "[concat_map] and [contra_concat_map]" =
    run_operation_test_for_all_producers_and_consumers
      ~operation:
        (Concat_map
           (fun x ->
             let xs = List.init x ~f:(const x) in
             print_s [%sexp "concat_map", (x : int), (xs : int list)];
             xs))
      ~expect:(fun () ->
        [%expect
          {|
          (producer (0 5))
          (concat_map 0 ())
          (concat_map 5 (5 5 5 5 5))
          (consumer (5 5 5 5 5))

          (producer (1 6))
          (concat_map 1 (1))
          (concat_map 6 (6 6 6 6 6 6))
          (consumer (1 6 6 6 6 6 6))

          (producer (2 7))
          (concat_map 2 (2 2))
          (concat_map 7 (7 7 7 7 7 7 7))
          (consumer (2 2 7 7 7 7 7 7 7))

          (producer (3 8))
          (concat_map 3 (3 3 3))
          (concat_map 8 (8 8 8 8 8 8 8 8))
          (consumer (3 3 3 8 8 8 8 8 8 8 8))

          (producer (4 9))
          (concat_map 4 (4 4 4 4))
          (concat_map 9 (9 9 9 9 9 9 9 9 9))
          (consumer (4 4 4 4 9 9 9 9 9 9 9 9 9))
          |}])
  ;;
end

(* We do not exhaustively re-test [of_pipe_reader], [of_pipe_writer], and
   [of_async_writer] here, because the implementation is mostly identical to that of
   global iterators, and we can again make the argument that if things like stop
   conditions and pushback work for all ['a Gel.t]s, then they must also work for all
   ['a Queue.t Gel.t]s. We include a couple demonstrations of batched-specific behavior to
   be safe. *)
module%test [@name "batched-specific [of_pipe_reader] and [of_pipe_writer] behavior"] _ =
struct
  let%expect_test "[of_pipe_reader] always reads all values available, regardless of \
                   [size_budget]"
    =
    let writer =
      Pipe.create_writer ~size_budget:0 (fun reader ->
        let producer = Iterator.Batched.of_pipe_reader reader in
        let consumer =
          Iterator.create_consumer
            ~f:(fun xs ->
              print_s [%sexp "consumer", (xs : int Queue.t)];
              Maybe_pushback.unit)
            ()
        in
        let%bind () = Iterator.start producer consumer >>| ok_exn in
        print_endline "stopped";
        return ())
    in
    Pipe.write_without_pushback writer 1;
    Pipe.write_without_pushback writer 2;
    Pipe.write_without_pushback writer 3;
    Pipe.close writer;
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    [%expect
      {|
      (consumer (1 2 3))
      stopped
      |}];
    return ()
  ;;

  let%expect_test "[of_pipe_writer] always writes all values available, regardless of \
                   [size_budget]"
    =
    let reader =
      Pipe.create_reader ~size_budget:0 ~close_on_exception:false (fun writer ->
        let producer =
          Sequence.range 0 10
          |> Fn.flip Sequence.chunks_exn 3
          |> Sequence.map ~f:Queue.of_list
          |> Iterator.of_sequence
        in
        let consumer = Iterator.Batched.of_pipe_writer writer in
        let%bind () = Iterator.start producer consumer >>| ok_exn in
        print_endline "stopped";
        return ())
    in
    let read_and_print () =
      let%bind xs = Pipe.read' reader in
      print_s [%sexp ~~(xs : [ `Ok of int Queue.t | `Eof ])];
      return ()
    in
    let%bind () = read_and_print () in
    [%expect {| (xs (Ok (0 1 2))) |}];
    let%bind () = read_and_print () in
    [%expect {| (xs (Ok (3 4 5))) |}];
    let%bind () = read_and_print () in
    [%expect {| (xs (Ok (6 7 8))) |}];
    Pipe.close_read reader;
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    [%expect {| stopped |}];
    return ()
  ;;
end

(* We do not test [of_direct_stream_writer] here because setting up such a test is more
   complicated and the implementation is fundamentally the same as [of_async_writer]. See
   [Async_iterator_rpc_tests] for tests exercising this function. *)
