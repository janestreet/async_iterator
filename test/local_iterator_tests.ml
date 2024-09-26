open! Core
open! Async
open! Import
open Shared_setup

let%template run_test_for_all_producers_and_operations ~iter ~iter' ~create_consumer =
  let%bind () =
    (run_test_for_all_producers [@mode local])
      ~iter
      ~iter'
      ~create_consumer
      ~operation:Ident
  in
  let%bind () =
    (run_test_for_all_producers [@mode local])
      ~iter
      ~iter'
      ~create_consumer
      ~operation:(Inspect (fun _ -> ()))
  in
  let%bind () =
    (run_test_for_all_producers [@mode local])
      ~iter
      ~iter'
      ~create_consumer
      ~operation:(Filter (fun _ -> true))
  in
  let%bind () =
    (run_test_for_all_producers [@mode local])
      ~iter
      ~iter'
      ~create_consumer
      ~operation:(Map (fun x -> x))
  in
  let%bind () =
    (run_test_for_all_producers [@mode local])
      ~iter
      ~iter'
      ~create_consumer
      ~operation:(Filter_map (fun x -> Some x))
  in
  let%bind () =
    (run_test_for_all_producers [@mode local])
      ~iter
      ~iter'
      ~create_consumer
      ~operation:(Concat_map (fun x -> [ x ]))
  in
  return ()
;;

let run_stop_test_for_all_producers_and_operations ~iter ~create_consumer =
  run_test_for_all_producers_and_operations
    ~iter:(fun ~f:_ ~stop -> iter ~stop)
    ~iter':(fun ~f:_ ~stop -> iter ~stop)
    ~create_consumer
;;

(* These tests demonstrate that stop conditions are propagated identically, up to
   differences between [Maybe_pushback] and [Action], regardless of the combination of
   producer, consumer, and operation. *)
let%test_module "stop conditions" =
  (module [%template
    let%expect_test "[stop] deferred" =
      run_stop_test_for_all_producers_and_operations
        ~create_consumer:(fun () ->
          (Iterator.create_consumer [@mode local])
            ~f:(fun () -> assert false)
            ~stop:(return ())
            ())
        ~iter:(fun ~stop ->
          print_stop stop;
          [%expect {| (stop Empty) |}];
          Deferred.ok stop)
    ;;

    let%expect_test "[Stop] action from [stop] deferred" =
      run_test_for_all_producers_and_operations
        ~create_consumer:(fun () ->
          (Iterator.create_consumer [@mode local])
            ~f:(fun () -> assert false)
            ~stop:(return ())
            ())
        ~iter:(fun ~f ~stop ->
          print_pushback (f ());
          [%expect {| (pushback (Full ())) |}];
          print_stop stop;
          [%expect {| (stop (Full ())) |}];
          return (Ok ()))
        ~iter':(fun ~f ~stop ->
          print_action (f ());
          [%expect {| (action Stop) |}];
          Deferred.ok stop)
    ;;

    let%expect_test "[stop] choices" =
      run_stop_test_for_all_producers_and_operations
        ~create_consumer:(fun () ->
          (Iterator.create_consumer' [@mode local])
            ~f:(fun () -> Continue)
            ~stop:[ choice (return ()) (fun () -> ()) ]
            ())
        ~iter:(fun ~stop ->
          print_stop stop;
          [%expect {| (stop Empty) |}];
          Deferred.ok stop)
    ;;

    let%expect_test "[Stop] action" =
      run_test_for_all_producers_and_operations
        ~create_consumer:(fun () ->
          (Iterator.create_consumer' [@mode local]) ~f:(fun () -> Stop) ~stop:[] ())
        ~iter:(fun ~f ~stop ->
          print_pushback (f ());
          [%expect {| (pushback (Full ())) |}];
          print_stop stop;
          [%expect {| (stop (Full ())) |}];
          return (Ok ()))
        ~iter':(fun ~f ~stop ->
          print_action (f ());
          [%expect {| (action Stop) |}];
          let%bind () = Scheduler.yield_until_no_jobs_remain () in
          print_stop stop;
          [%expect {| (stop Empty) |}];
          return (Ok ()))
    ;;])
;;

let%template run_pushback_test_for_all_producers_consumers_and_operations ~iter ~iter' =
  let%bind () =
    run_test_for_all_producers_and_operations
      ~create_consumer:(fun () ->
        (Iterator.create_consumer [@mode local])
          ~f:(function
            | false -> Maybe_pushback.unit
            | true -> Maybe_pushback.of_deferred (never ()))
          ())
      ~iter
      ~iter'
  in
  let%bind () =
    run_test_for_all_producers_and_operations
      ~create_consumer:(fun () ->
        (Iterator.create_consumer' [@mode local])
          ~f:(function
            | false -> Continue
            | true -> Wait { pushback = never () })
          ())
      ~iter
      ~iter'
  in
  return ()
;;

(* This test demonstrates that pushback is propagated identically, up to differences
   between [Maybe_pushback] and [Action], regardless of the combination of producer,
   consumer, and operation. *)
let%test_module "pushback" =
  (module struct
    let%expect_test _ =
      run_pushback_test_for_all_producers_consumers_and_operations
        ~iter:(fun ~f ~stop:_ ->
          print_pushback (f false);
          [%expect {| (pushback (Full ())) |}];
          print_pushback (f true);
          [%expect {| (pushback Empty) |}];
          return (Ok ()))
        ~iter':(fun ~f ~stop:_ ->
          print_action (f false);
          [%expect {| (action Continue) |}];
          print_action (f true);
          [%expect {| (action (Wait (pushback Empty))) |}];
          return (Ok ()))
    ;;
  end)
;;

let%test_module "staged-specific behavior" =
  (module struct
    let%expect_test "it is an error to call [f] before a staged producer is started" =
      let run_test ~create_producer =
        let%bind () =
          Expect_test_helpers_async.require_does_raise_async
            ~hide_positions:true
            create_producer
        in
        [%expect
          {| ("[Set_once.get_exn] unset" (at lib/async_iterator/src/iterator.ml:LINE:COL)) |}];
        return ()
      in
      let%bind () =
        run_test ~create_producer:(fun () ->
          Iterator.create_producer_staged ~iter:(fun ~start ~f ~stop:_ ->
            print_s [%sexp "start", (start : unit Deferred.t)];
            [%expect {| (start Empty) |}];
            ignore (f () : unit Maybe_pushback.t);
            return (Ok (return (Ok ())))))
      in
      let%bind () =
        run_test ~create_producer:(fun () ->
          Iterator.create_producer_staged'
            ~iter:(fun ~f ->
              ignore (f () : Iterator.Action.t);
              return (Ok ()))
            ~start:(fun () ~stop:_ -> return (Ok ())))
      in
      return ()
    ;;
  end)
;;

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

(* These tests demonstrate that each operation (filter, map, etc.) and its
   "contra"-prefixed dual have the same behavior, and that this behavior is implemented
   correctly. *)
let%test_module "basic operations" =
  (module struct
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

let%test_module "operation-specific semantics" =
  (module struct
    let%expect_test "filtering out messages always results in a continue" =
      let run_test_for_all_producers ~check_message ~operation =
        run_test_for_all_producers
          ~iter:(fun ~f ~stop ->
            print_pushback (f false);
            [%expect {| (pushback (Full ())) |}];
            print_pushback (f true);
            [%expect {| (pushback Empty) |}];
            let%bind () = stop in
            print_pushback (f false);
            [%expect {| (pushback (Full ())) |}];
            print_pushback (f true);
            [%expect {| (pushback (Full ())) |}];
            return (Ok ()))
          ~iter':(fun ~f ~stop ->
            print_action (f false);
            [%expect {| (action Continue) |}];
            print_action (f true);
            [%expect {| (action (Wait (pushback Empty))) |}];
            let%bind () = stop in
            print_action (f false);
            [%expect {| (action Continue) |}];
            print_action (f true);
            [%expect {| (action Stop) |}];
            return (Ok ()))
          ~create_consumer:(fun () ->
            let stop = Ivar.create () in
            Iterator.create_consumer
              ~f:(fun message ->
                check_message message;
                Ivar.fill_exn stop ();
                Maybe_pushback.of_deferred (never ()))
              ~stop:(Ivar.read stop)
              ())
          ~operation
      in
      let%bind () =
        run_test_for_all_producers
          ~check_message:(fun true_ -> assert true_)
          ~operation:(Filter Fn.id)
      in
      let%bind () =
        run_test_for_all_producers
          ~check_message:Fn.id
          ~operation:
            (Filter_map
               (function
                 | false -> None
                 | true -> Some ()))
      in
      let%bind () =
        run_test_for_all_producers
          ~check_message:Fn.id
          ~operation:
            (Concat_map
               (function
                 | false -> []
                 | true -> [ () ]))
      in
      return ()
    ;;

    module Batch = struct
      module Speed = struct
        type t =
          | Immediate
          | Fast
          | Slow
        [@@deriving enumerate]
      end

      module Elt = struct
        type t =
          { mutable speed : Speed.t
          ; mvar : (unit, read_write) Mvar.t
          }

        let create () = { speed = Immediate; mvar = Mvar.create () }

        let set_speed t ~speed =
          assert (Mvar.is_empty t.mvar);
          t.speed <- speed;
          match speed with
          | Immediate -> Mvar.set t.mvar ()
          | Slow | Fast -> ()
        ;;

        let pushback t = Maybe_pushback.of_deferred (Mvar.take t.mvar)

        let fire_pushback_and_wait t ~(speed : Speed.t) =
          match t.speed, speed with
          | Immediate, (Immediate | Fast | Slow)
          | Fast, (Immediate | Slow)
          | Slow, (Immediate | Fast) -> return ()
          | Slow, Slow | Fast, Fast ->
            assert (Mvar.is_empty t.mvar);
            Mvar.set t.mvar ();
            let%bind () = Scheduler.yield_until_no_jobs_remain () in
            assert (Mvar.is_empty t.mvar);
            return ()
        ;;
      end

      type t =
        { fst : Elt.t
        ; snd : Elt.t
        }
      [@@deriving typed_fields]

      let create () = { fst = Elt.create (); snd = Elt.create () }

      let set_speeds t ~fst ~snd =
        Elt.set_speed t.fst ~speed:fst;
        Elt.set_speed t.snd ~speed:snd
      ;;

      let pushback t ~which:{ Typed_field.Packed.f = T ((Fst | Snd) as which) } =
        Elt.pushback (Typed_field.get which t)
      ;;

      let fire_pushback_and_wait { fst; snd } ~speed =
        let%bind () = Elt.fire_pushback_and_wait fst ~speed
        and () = Elt.fire_pushback_and_wait snd ~speed in
        return ()
      ;;
    end

    let%expect_test "[concat_map] and [contra_concat_map]: pushback is per-batch" =
      let batch = Batch.create () in
      let run_test ~f ~fst ~snd ~expect =
        Batch.set_speeds batch ~fst ~snd;
        let%bind () = Batch.fire_pushback_and_wait batch ~speed:Immediate in
        print_endline "fired [Immediate]";
        let pushback = f () in
        print_pushback pushback;
        let%bind () = Batch.fire_pushback_and_wait batch ~speed:Fast in
        print_endline "fired [Fast]";
        print_pushback pushback;
        let%bind () = Batch.fire_pushback_and_wait batch ~speed:Slow in
        print_endline "fired [Slow]";
        print_pushback pushback;
        expect ();
        return ()
      in
      let iter_all_pushback_permutations ~f =
        Deferred.List.iter
          ~how:`Sequential
          [%all: Batch.Speed.t * Batch.Speed.t]
          ~f:(fun (fst, snd) ->
            let run_test = run_test ~f ~fst ~snd in
            match fst, snd with
            | Immediate, Immediate ->
              run_test ~expect:(fun () ->
                [%expect
                  {|
                  fired [Immediate]
                  (pushback (Full ()))
                  fired [Fast]
                  (pushback (Full ()))
                  fired [Slow]
                  (pushback (Full ()))
                  |}])
            | Fast, (Immediate | Fast) | Immediate, Fast ->
              run_test ~expect:(fun () ->
                [%expect
                  {|
                  fired [Immediate]
                  (pushback Empty)
                  fired [Fast]
                  (pushback (Full ()))
                  fired [Slow]
                  (pushback (Full ()))
                  |}])
            | Slow, (Immediate | Fast | Slow) | (Immediate | Fast), Slow ->
              run_test ~expect:(fun () ->
                [%expect
                  {|
                  fired [Immediate]
                  (pushback Empty)
                  fired [Fast]
                  (pushback Empty)
                  fired [Slow]
                  (pushback (Full ()))
                  |}]))
      in
      run_test_for_all_producers
        ~iter:(fun ~f ~stop:_ -> iter_all_pushback_permutations ~f |> Deferred.ok)
        ~iter':(fun ~f ~stop:_ ->
          iter_all_pushback_permutations ~f:(fun () ->
            match f () with
            | Continue -> Maybe_pushback.unit
            | Wait { pushback } -> Maybe_pushback.of_deferred pushback
            | Stop -> assert false)
          |> Deferred.ok)
        ~create_consumer:(fun () ->
          Iterator.create_consumer ~f:(fun which -> Batch.pushback batch ~which) ())
        ~operation:(Concat_map (fun () -> Batch.Typed_field.Packed.all))
    ;;
  end)
;;
