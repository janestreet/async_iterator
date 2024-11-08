open! Core
open! Async
open! Import

[%%template
[@@@mode m = (global, local)]

let[@mode m] run_one_test_with_producer_and_consumer_operation
  ~create_producer
  ~create_consumer
  ~operation
  =
  let%bind () =
    let%bind producer = create_producer () >>| ok_exn in
    let consumer = create_consumer () in
    Iterator.start ((Operation.apply [@mode m]) operation producer) consumer >>| ok_exn
  in
  let%bind () =
    let%bind producer = create_producer () >>| ok_exn in
    let consumer = create_consumer () in
    Iterator.start producer ((Operation.contra_apply [@mode m]) operation consumer)
    >>| ok_exn
  in
  return ()
;;

let[@mode m] gen_run_test_for_all_producers
  ~run_one_test
  ~iter
  ~iter'
  ~create_consumer
  ~operation
  =
  let%bind () =
    run_one_test
      ~create_producer:(fun () ->
        return (Ok ((Iterator.create_producer [@mode m]) ~iter)))
      ~create_consumer
      ~operation
  in
  let%bind () =
    run_one_test
      ~create_producer:(fun () ->
        return
          (Ok
             ((Iterator.create_producer' [@mode m]) ~iter:(fun ~f ~stop ->
                iter' ~f ~stop:(choose stop)))))
      ~create_consumer
      ~operation
  in
  let%bind () =
    run_one_test
      ~create_producer:(fun () ->
        (Iterator.create_producer_staged [@mode m]) ~iter:(fun ~start ~f ~stop ->
          let stopped =
            let%bind () = start in
            iter ~f ~stop
          in
          return (Ok stopped)))
      ~create_consumer
      ~operation
  in
  let%bind () =
    run_one_test
      ~create_producer:(fun () ->
        (Iterator.create_producer_staged' [@mode m])
          ~iter:(fun ~f -> return (Ok f))
          ~start:(fun f ~stop -> iter' ~f ~stop:(choose stop)))
      ~create_consumer
      ~operation
  in
  return ()
;;

let[@mode m] run_test_for_all_producers ~iter ~iter' ~create_consumer ~operation =
  (gen_run_test_for_all_producers [@mode m])
    ~run_one_test:(run_one_test_with_producer_and_consumer_operation [@mode m])
    ~iter
    ~iter'
    ~create_consumer
    ~operation
;;]

module Batched = struct
  let run_one_test_with_producer_and_consumer_operation
    ~create_producer
    ~create_consumer
    ~operation
    =
    let%bind () =
      let%bind producer = create_producer () >>| ok_exn in
      let consumer = create_consumer () in
      Iterator.start (Operation.Batched.apply operation producer) consumer >>| ok_exn
    in
    let%bind () =
      let%bind producer = create_producer () >>| ok_exn in
      let consumer = create_consumer () in
      Iterator.start producer (Operation.Batched.contra_apply operation consumer)
      >>| ok_exn
    in
    return ()
  ;;

  let run_test_for_all_producers ~iter ~iter' ~create_consumer ~operation =
    gen_run_test_for_all_producers
      ~run_one_test:run_one_test_with_producer_and_consumer_operation
      ~iter
      ~iter'
      ~create_consumer
      ~operation
  ;;
end
