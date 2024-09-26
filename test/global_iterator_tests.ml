open! Core
open! Async
open! Import

(* Unlike [Local_iterator_tests], these tests do not exhaustively check the various
   [create] functions and operations ([filter], [map], and so on). Since global iterators
   are a subtype of local iterators, we make the argument if something works for all ['a],
   then it must work just as well for all ['a Modes.Global.t].

   Besides, the implementations are all produced via ppx_template, so the code is
   identical modulo mode polymorphism.

   To be safe, we include a couple global-specific tests of basic functionality. *)
let%test_module "basic operations" =
  (module struct
    open Shared_setup

    let run_operation_test_for_all_producers_and_consumers ~operation ~expect =
      let run_operation_test_for_all_producers ~create_consumer =
        run_test_for_all_producers
          ~iter:(fun ~f ~stop:_ ->
            for x = 0 to 4 do
              print_s [%sexp "producer", (x : int)];
              let pushback = f x in
              assert (Deferred.is_determined (Maybe_pushback.to_deferred pushback));
              print_newline ()
            done;
            expect ();
            return (Ok ()))
          ~iter':(fun ~f ~stop:_ ->
            for x = 0 to 4 do
              print_s [%sexp "producer", (x : int)];
              let action = f x in
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
            ~f:(fun x ->
              print_s [%sexp "consumer", (x : int)];
              Maybe_pushback.unit)
            ())
      in
      let%bind () =
        run_operation_test_for_all_producers ~create_consumer:(fun () ->
          Iterator.create_consumer'
            ~f:(fun x ->
              print_s [%sexp "consumer", (x : int)];
              Continue)
            ())
      in
      return ()
    ;;

    let%expect_test "no operation" =
      run_operation_test_for_all_producers_and_consumers
        ~operation:Ident
        ~expect:(fun () ->
          [%expect
            {|
            (producer 0)
            (consumer 0)

            (producer 1)
            (consumer 1)

            (producer 2)
            (consumer 2)

            (producer 3)
            (consumer 3)

            (producer 4)
            (consumer 4)
            |}])
    ;;

    let%expect_test "[inspect] and [contra_inspect]" =
      run_operation_test_for_all_producers_and_consumers
        ~operation:(Inspect (fun x -> print_s [%sexp "inspect", (x : int)]))
        ~expect:(fun () ->
          [%expect
            {|
            (producer 0)
            (inspect 0)
            (consumer 0)

            (producer 1)
            (inspect 1)
            (consumer 1)

            (producer 2)
            (inspect 2)
            (consumer 2)

            (producer 3)
            (inspect 3)
            (consumer 3)

            (producer 4)
            (inspect 4)
            (consumer 4)
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
            (producer 0)
            (filter 0 true)
            (consumer 0)

            (producer 1)
            (filter 1 false)

            (producer 2)
            (filter 2 true)
            (consumer 2)

            (producer 3)
            (filter 3 false)

            (producer 4)
            (filter 4 true)
            (consumer 4)
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
            (producer 0)
            (map 0 0)
            (consumer 0)

            (producer 1)
            (map 1 -1)
            (consumer -1)

            (producer 2)
            (map 2 -2)
            (consumer -2)

            (producer 3)
            (map 3 -3)
            (consumer -3)

            (producer 4)
            (map 4 -4)
            (consumer -4)
            |}])
    ;;

    let%expect_test "[filter_map] and [contra_filter_map]" =
      run_operation_test_for_all_producers_and_consumers
        ~operation:
          (Filter_map
             (fun x ->
               let negative_x = if x % 2 = 0 then Some (-x) else None in
               print_s
                 [%sexp
                   "filter_map", (x : int), (negative_x : (int option[@sexp.option]))];
               negative_x))
        ~expect:(fun () ->
          [%expect
            {|
            (producer 0)
            (filter_map 0 0)
            (consumer 0)

            (producer 1)
            (filter_map 1)

            (producer 2)
            (filter_map 2 -2)
            (consumer -2)

            (producer 3)
            (filter_map 3)

            (producer 4)
            (filter_map 4 -4)
            (consumer -4)
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
            (producer 0)
            (concat_map 0 ())

            (producer 1)
            (concat_map 1 (1))
            (consumer 1)

            (producer 2)
            (concat_map 2 (2 2))
            (consumer 2)
            (consumer 2)

            (producer 3)
            (concat_map 3 (3 3 3))
            (consumer 3)
            (consumer 3)
            (consumer 3)

            (producer 4)
            (concat_map 4 (4 4 4 4))
            (consumer 4)
            (consumer 4)
            (consumer 4)
            (consumer 4)
            |}])
    ;;
  end)
;;

let%test_module "[of_sequence]" =
  (module struct
    let%expect_test "[stop] deferred" =
      let producer = Iterator.of_sequence (Sequence.repeat ()) in
      let consumer =
        Iterator.create_consumer ~f:(fun () -> assert false) ~stop:(return ()) ()
      in
      Iterator.start producer consumer >>| ok_exn
    ;;

    let%expect_test "[Stop] action" =
      let producer = Iterator.of_sequence (Sequence.repeat ()) in
      let consumer = Iterator.create_consumer' ~f:(fun () -> Stop) () in
      Iterator.start producer consumer >>| ok_exn
    ;;

    let%expect_test "[stop] choices alone cannot stop iterator" =
      let producer = Iterator.of_sequence (Sequence.range 1 4) in
      let consumer =
        Iterator.create_consumer'
          ~f:(fun x ->
            print_s [%sexp "consumer", (x : int)];
            Continue)
          ~stop:[ choice (return ()) Fn.id ]
          ()
      in
      let%bind () = Iterator.start producer consumer >>| ok_exn in
      [%expect
        {|
        (consumer 1)
        (consumer 2)
        (consumer 3)
        |}];
      return ()
    ;;

    let%expect_test "waits for pushback" =
      let pushback = Bvar.create () in
      let producer = Iterator.of_sequence (Sequence.range 1 4) in
      let consumer =
        Iterator.create_consumer
          ~f:(fun x ->
            print_s [%sexp "consumer", (x : int)];
            Maybe_pushback.of_deferred (Bvar.wait pushback))
          ()
      in
      let stopped = Iterator.start producer consumer in
      [%expect {| (consumer 1) |}];
      print_stopped stopped;
      [%expect {| (stopped Empty) |}];
      Bvar.broadcast pushback ();
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| (consumer 2) |}];
      print_stopped stopped;
      [%expect {| (stopped Empty) |}];
      Bvar.broadcast pushback ();
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| (consumer 3) |}];
      print_stopped stopped;
      [%expect {| (stopped Empty) |}];
      Bvar.broadcast pushback ();
      stopped >>| ok_exn
    ;;

    let%expect_test "exceptions are not caught" =
      let producer = Iterator.of_sequence (Sequence.repeat ()) in
      let consumer = Iterator.create_consumer ~f:(fun () -> failwith "oops") () in
      let%bind () =
        Expect_test_helpers_async.require_does_raise_async (fun () ->
          Iterator.start producer consumer)
      in
      [%expect {| (Failure oops) |}];
      return ()
    ;;
  end)
;;

let%test_module "[of_pipe_reader]" =
  (module struct
    module Flushed = struct
      type t =
        | When_value_read
        | When_value_processed
        | Consumer of { downstream_flushed : unit -> Pipe.Flushed_result.t Deferred.t }

      let to_pipe_flushed ~reader : t -> Pipe.Flushed.t = function
        | When_value_read -> When_value_read
        | When_value_processed -> When_value_processed
        | Consumer { downstream_flushed } ->
          Consumer (Pipe.add_consumer reader ~downstream_flushed)
      ;;
    end

    let create_writer ?flushed ~consumer () =
      Pipe.create_writer (fun reader ->
        let flushed = Option.map ~f:(Flushed.to_pipe_flushed ~reader) flushed in
        let producer = Iterator.of_pipe_reader ?flushed reader in
        let%bind () = Iterator.start producer consumer >>| ok_exn in
        print_endline "stopped";
        return ())
    ;;

    let%expect_test "[stop] deferred closes pipe" =
      let writer =
        create_writer
          ~consumer:
            (Iterator.create_consumer ~f:(fun _ -> assert false) ~stop:(return ()) ())
          ()
      in
      let closed = Pipe.closed writer in
      print_closed closed;
      [%expect {| (closed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      print_closed closed;
      [%expect {| (closed (Full ())) |}];
      return ()
    ;;

    let%expect_test "[Stop] action closes pipe" =
      let writer =
        create_writer ~consumer:(Iterator.create_consumer' ~f:(fun _ -> Stop) ()) ()
      in
      let closed = Pipe.closed writer in
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_closed closed;
      [%expect {| (closed Empty) |}];
      Pipe.write_without_pushback_if_open writer ();
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      print_closed closed;
      [%expect {| (closed (Full ())) |}];
      return ()
    ;;

    let%expect_test "empty pipe w/o consumer is immediately flushed" =
      let writer =
        create_writer
          ~flushed:When_value_read
          ~consumer:(Iterator.create_consumer ~f:(fun _ -> assert false) ())
          ()
      in
      print_flushed (Pipe.downstream_flushed writer);
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "empty pipe w/ trivial consumer is immediately flushed" =
      let writer =
        create_writer
          ~flushed:When_value_processed
          ~consumer:(Iterator.create_consumer ~f:(fun _ -> assert false) ())
          ()
      in
      let flushed = Pipe.downstream_flushed writer in
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "empty pipe w/ custom consumer is flushed when consumer says so" =
      let downstream_flushed = Ivar.create () in
      let writer =
        create_writer
          ~flushed:
            (Consumer { downstream_flushed = (fun () -> Ivar.read downstream_flushed) })
          ~consumer:(Iterator.create_consumer ~f:(fun _ -> assert false) ())
          ()
      in
      let flushed = Pipe.downstream_flushed writer in
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      Ivar.fill_exn downstream_flushed `Ok;
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "closed pipe w/o consumer is immediately flushed" =
      let writer =
        create_writer
          ~flushed:When_value_read
          ~consumer:
            (Iterator.create_consumer ~f:(fun _ -> assert false) ~stop:(return ()) ())
          ()
      in
      print_flushed (Pipe.downstream_flushed writer);
      [%expect {| (flushed (Full Ok)) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      print_flushed (Pipe.downstream_flushed writer);
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "closed pipe w/ trivial consumer is immediately flushed" =
      let writer =
        create_writer
          ~flushed:When_value_processed
          ~consumer:
            (Iterator.create_consumer ~f:(fun _ -> assert false) ~stop:(return ()) ())
          ()
      in
      let before_close = Pipe.downstream_flushed writer in
      print_flushed before_close;
      [%expect {| (flushed (Full Ok)) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      print_flushed before_close;
      [%expect {| (flushed (Full Ok)) |}];
      let after_close = Pipe.downstream_flushed writer in
      print_flushed after_close;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed after_close;
      [%expect {| (flushed (Full Reader_closed)) |}];
      return ()
    ;;

    let%expect_test "closed pipe w/ custom consumer is flushed when consumer says so" =
      let downstream_flushed = Mvar.create () in
      let writer =
        create_writer
          ~flushed:
            (Consumer { downstream_flushed = (fun () -> Mvar.take downstream_flushed) })
          ~consumer:
            (Iterator.create_consumer ~f:(fun _ -> assert false) ~stop:(return ()) ())
          ()
      in
      let before_close = Pipe.downstream_flushed writer in
      print_flushed before_close;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      print_flushed before_close;
      [%expect {| (flushed Empty) |}];
      Mvar.set downstream_flushed `Ok;
      print_flushed before_close;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed before_close;
      [%expect {| (flushed (Full Ok)) |}];
      let after_close = Pipe.downstream_flushed writer in
      print_flushed after_close;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed after_close;
      [%expect {| (flushed Empty) |}];
      Mvar.set downstream_flushed `Reader_closed;
      print_flushed after_close;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed after_close;
      [%expect {| (flushed (Full Reader_closed)) |}];
      return ()
    ;;

    let%expect_test "flushed when value read" =
      let writer =
        create_writer
          ~flushed:When_value_read
          ~consumer:
            (Iterator.create_consumer
               ~f:(fun () ->
                 print_s [%sexp "consumer"];
                 Maybe_pushback.of_deferred (never ()))
               ())
          ()
      in
      Pipe.write_without_pushback writer ();
      let flushed = Pipe.downstream_flushed writer in
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| consumer |}];
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "flushed when value processed" =
      let pushback = Ivar.create () in
      let writer =
        create_writer
          ~flushed:When_value_processed
          ~consumer:
            (Iterator.create_consumer
               ~f:(fun () ->
                 print_s [%sexp "consumer"];
                 Maybe_pushback.of_deferred (Ivar.read pushback))
               ())
          ()
      in
      Pipe.write_without_pushback writer ();
      let flushed = Pipe.downstream_flushed writer in
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| consumer |}];
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      Ivar.fill_exn pushback ();
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "flushed when consumer says so" =
      let downstream_flushed = Ivar.create () in
      let writer =
        create_writer
          ~flushed:
            (Consumer { downstream_flushed = (fun () -> Ivar.read downstream_flushed) })
          ~consumer:
            (Iterator.create_consumer
               ~f:(fun () ->
                 print_s [%sexp "consumer"];
                 Maybe_pushback.unit)
               ())
          ()
      in
      Pipe.write_without_pushback writer ();
      let flushed = Pipe.downstream_flushed writer in
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| consumer |}];
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      Ivar.fill_exn downstream_flushed `Ok;
      print_flushed flushed;
      [%expect {| (flushed Empty) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      print_flushed flushed;
      [%expect {| (flushed (Full Ok)) |}];
      return ()
    ;;

    let%expect_test "all available messages are read from closed pipe" =
      let writer =
        create_writer
          ~consumer:
            (Iterator.create_consumer
               ~f:(fun x ->
                 print_s [%sexp "consumer", (x : int)];
                 Maybe_pushback.unit)
               ())
          ()
      in
      Pipe.write_without_pushback writer 1;
      Pipe.write_without_pushback writer 2;
      Pipe.write_without_pushback writer 3;
      Pipe.close writer;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect
        {|
        (consumer 1)
        (consumer 2)
        (consumer 3)
        stopped
        |}];
      return ()
    ;;

    let%expect_test "iterator waits for messages" =
      let writer =
        create_writer
          ~consumer:
            (Iterator.create_consumer
               ~f:(fun x ->
                 print_s [%sexp "consumer", (x : int)];
                 Maybe_pushback.unit)
               ())
          ()
      in
      Pipe.write_without_pushback writer 1;
      Pipe.write_without_pushback writer 2;
      Pipe.write_without_pushback writer 3;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect
        {|
        (consumer 1)
        (consumer 2)
        (consumer 3)
        |}];
      Pipe.write_without_pushback writer 4;
      Pipe.write_without_pushback writer 5;
      Pipe.write_without_pushback writer 6;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect
        {|
        (consumer 4)
        (consumer 5)
        (consumer 6)
        |}];
      return ()
    ;;

    let%expect_test "iterator stops if pipe closed while waiting for messages" =
      let writer =
        create_writer
          ~consumer:
            (Iterator.create_consumer
               ~f:(fun x ->
                 print_s [%sexp "consumer", (x : int)];
                 Maybe_pushback.unit)
               ())
          ()
      in
      Pipe.write_without_pushback writer 1;
      Pipe.write_without_pushback writer 2;
      Pipe.write_without_pushback writer 3;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect
        {|
        (consumer 1)
        (consumer 2)
        (consumer 3)
        |}];
      Pipe.close writer;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      return ()
    ;;

    let%expect_test "exceptions are not caught" =
      let writer =
        Pipe.create_writer (fun reader ->
          let producer = Iterator.of_pipe_reader reader in
          let consumer = Iterator.create_consumer ~f:(fun _ -> failwith "oops") () in
          Expect_test_helpers_async.require_does_raise_async (fun () ->
            Iterator.start producer consumer))
      in
      Pipe.write_without_pushback writer ();
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| (Failure oops) |}];
      return ()
    ;;
  end)
;;

let%test_module "[of_pipe_writer]" =
  (module struct
    let create_reader ~producer ?size_budget () =
      Pipe.create_reader ?size_budget ~close_on_exception:false (fun writer ->
        let consumer = Iterator.of_pipe_writer writer in
        let%bind () = Iterator.start producer consumer >>| ok_exn in
        print_endline "stopped";
        return ())
    ;;

    let%expect_test "[closed] stops asynchronously" =
      let reader =
        create_reader
          ~producer:(Iterator.create_producer ~iter:(fun ~f:_ ~stop -> Deferred.ok stop))
          ()
      in
      Pipe.close_read reader;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      return ()
    ;;

    let%expect_test "[closed] stops synchronously" =
      let the_reader = Ivar.create () in
      let reader =
        create_reader
          ~producer:
            (Iterator.create_producer' ~iter:(fun ~f ~stop ->
               print_action (f ());
               [%expect {| (action (Wait (pushback Empty))) |}];
               let%bind reader = Ivar.read the_reader in
               Pipe.close_read reader;
               print_action (f ());
               [%expect {| (action Stop) |}];
               Deferred.ok (choose stop)))
          ()
      in
      Ivar.fill_exn the_reader reader;
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      return ()
    ;;

    let%expect_test "respects [size_budget]" =
      let reader =
        create_reader
          ~producer:(Iterator.of_sequence (Sequence.range 1 10))
          ~size_budget:2
          ()
      in
      let read_and_print_all () =
        let%bind xs = Pipe.read' reader in
        print_s [%sexp ~~(xs : [ `Ok of int Queue.t | `Eof ])];
        return ()
      in
      let%bind () = read_and_print_all () in
      [%expect {| (xs (Ok (1 2 3))) |}];
      let%bind () = read_and_print_all () in
      [%expect {| (xs (Ok (4 5 6))) |}];
      let%bind () = read_and_print_all () in
      [%expect {| (xs (Ok (7 8 9))) |}];
      let%bind () = Scheduler.yield_until_no_jobs_remain () in
      [%expect {| stopped |}];
      return ()
    ;;
  end)
;;

let%test_module "[of_async_writer]" =
  (module struct
    let%expect_test "[closed] stops asynchronously" =
      Writer.with_file "/dev/null" ~f:(fun writer ->
        let producer =
          Iterator.create_producer ~iter:(fun ~f:_ ~stop -> Deferred.ok stop)
        in
        let consumer = Iterator.of_async_writer writer ~write:(fun _ _ -> ()) in
        let%bind () = Writer.close writer
        and () = Iterator.start producer consumer >>| ok_exn in
        return ())
    ;;

    let%expect_test "[closed] stops synchronously" =
      Writer.with_file "/dev/null" ~f:(fun writer ->
        let producer =
          Iterator.create_producer' ~iter:(fun ~f ~stop ->
            print_action (f "");
            [%expect {| (action Continue) |}];
            don't_wait_for (Writer.close writer);
            print_action (f "");
            [%expect {| (action Stop) |}];
            Deferred.ok (choose stop))
        in
        let consumer = Iterator.of_async_writer writer ~write:Writer.write in
        Iterator.start producer consumer >>| ok_exn)
    ;;

    let%expect_test "respects [flush_every]" =
      Writer.with_file "/dev/null" ~f:(fun writer ->
        let producer =
          Iterator.create_producer ~iter:(fun ~f ~stop ->
            let%bind () =
              Eager_deferred.List.iter ~how:`Sequential (List.range 1 10) ~f:(fun _ ->
                let pushback = f " " in
                print_pushback pushback;
                Maybe_pushback.to_deferred pushback)
            in
            [%expect
              {|
              (pushback (Full ()))
              (pushback (Full ()))
              (pushback Empty)
              (pushback (Full ()))
              (pushback (Full ()))
              (pushback Empty)
              (pushback (Full ()))
              (pushback (Full ()))
              (pushback Empty)
              |}];
            don't_wait_for (Writer.close writer);
            Deferred.ok stop)
        in
        let consumer =
          Iterator.of_async_writer ~flush_every:3 writer ~write:Writer.write
        in
        Iterator.start producer consumer >>| ok_exn)
    ;;

    let%expect_test "[on_flush] can push back" =
      Writer.with_file "/dev/null" ~f:(fun writer ->
        let pushback = Mvar.create () in
        let producer =
          Iterator.create_producer ~iter:(fun ~f ~stop ->
            let%bind () =
              Eager_deferred.List.iter ~how:`Sequential (List.range 1 10) ~f:(fun _ ->
                let pushback = f " " in
                print_pushback pushback;
                Maybe_pushback.to_deferred pushback)
            in
            don't_wait_for (Writer.close writer);
            Deferred.ok stop)
        in
        let consumer =
          Iterator.of_async_writer
            ~flush_every:3
            ~on_flush:(fun () -> Maybe_pushback.of_deferred (Mvar.take pushback))
            writer
            ~write:Writer.write
        in
        let stopped = Iterator.start producer consumer in
        [%expect
          {|
          (pushback (Full ()))
          (pushback (Full ()))
          (pushback Empty)
          |}];
        let%bind () = Mvar.put pushback () in
        let%bind () = Writer.flushed writer in
        [%expect
          {|
          (pushback (Full ()))
          (pushback (Full ()))
          (pushback Empty)
          |}];
        let%bind () = Mvar.put pushback () in
        let%bind () = Writer.flushed writer in
        [%expect
          {|
          (pushback (Full ()))
          (pushback (Full ()))
          (pushback Empty)
          |}];
        let%bind () = Scheduler.yield_until_no_jobs_remain () in
        print_stopped stopped;
        [%expect {| (stopped Empty) |}];
        let%bind () = Mvar.put pushback () in
        stopped >>| ok_exn)
    ;;
  end)
;;

(* We do not test [of_direct_stream_writer] here because setting up such a test is more
   complicated and the implementation is fundamentally the same as [of_async_writer]. See
   [Async_iterator_rpc_tests] for tests exercising this function. *)

let%expect_test "[pre_sequence] and [post_sequence] behavior" =
  let pushback = Bvar.create () in
  let consumer =
    Iterator.create_consumer
      ~f:(fun i ->
        printf "%d\n" i;
        if i mod 2 = 1
        then Bvar.wait pushback |> Maybe_pushback.of_deferred
        else Maybe_pushback.unit)
      ()
  in
  (* This interleaves two sequences, [0;2;4] and [1;3;5]. The exact order in which they
     are interleaved is a bit weird but comes down to async semantics, e.g. the first two
     [upons] on an ivar run in reverse order, but subsequent [upon]s will run in the order
     they are added. *)
  let run_test consumer =
    let stopped =
      [ Iterator.of_sequence (Sequence.range ~stride:2 1 6)
      ; Iterator.of_sequence (Sequence.range ~stride:2 0 5)
      ]
      |> List.map ~f:(fun producer -> Iterator.start producer consumer)
      |> Deferred.Or_error.all_unit
      >>| ok_exn
    in
    let%bind () = Scheduler.yield_until_no_jobs_remain () in
    Deferred.repeat_until_finished () (fun () ->
      match Deferred.peek stopped with
      | None ->
        print_endline "pushback";
        Bvar.broadcast pushback ();
        let%bind () = Scheduler.yield_until_no_jobs_remain () in
        return (`Repeat ())
      | Some () -> return (`Finished ()))
  in
  (* The vanilla iterator only has pushback for odd numbers, so it processes [1] and waits
     for pushback on that sequence, but in the meantime it's able to eagerly process all
     of [0;2;4]. Then it processes [3;5] with pauses for pushback in between. *)
  let%bind () = run_test consumer in
  [%expect
    {|
    1
    0
    2
    4
    pushback
    3
    pushback
    5
    pushback
    |}];
  (* The pre-sequenced iterator waits for all previous interleaved values to stop pushing
     back before processing a new value, so it processes [1], then waits for pushback
     before processing [0], which doesn't push back so it processes [3], then waits for
     pushback before processing [2], etc. *)
  let%bind () = run_test (Iterator.pre_sequence consumer) in
  [%expect
    {|
    1
    pushback
    0
    3
    pushback
    2
    5
    pushback
    4
    |}];
  (* The post-sequenced iterator waits for all previous interleaved values to stop pushing
     back after processing a new value, so it processes [1], then eagerly processes [0],
     then waits for pushback from [1] before processing [2;4] in one go (no pushback),
     then waits for pushback after processing each of [3] and [5]. *)
  let%bind () =
    (consumer :> [%template: (int Iterator.Consumer.t[@mode local])])
    |> Iterator.post_sequence
    |> Iterator.coerce_consumer
    |> run_test
  in
  [%expect
    {|
    1
    0
    pushback
    2
    4
    3
    pushback
    5
    pushback
    |}];
  return ()
;;
