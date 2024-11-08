open! Core
open! Async
open! Import

module Message = struct
  module type Payload = sig
    type t [@@deriving bin_io, sexp_of]
  end

  type 'payload t =
    { worker : int
    ; payload : 'payload
    }
  [@@deriving bin_io, sexp_of]
end

module Args = struct
  type t =
    { number_of_workers : int
    ; worker : int
    }
  [@@deriving bin_io, fields ~getters]
end

type ('a, 'a_producer, 'payload, 'payload_producer) iterator =
  | Global :
      ( 'a
        , 'a Message.t Iterator.Producer.t
        , 'payload
        , 'payload Message.t Iterator.Producer.t )
        iterator
  | Batched :
      ( 'a
        , 'a Message.t Iterator.Batched.Producer.t
        , 'payload
        , 'payload Message.t Iterator.Batched.Producer.t )
        iterator

let gen_iter_with
  (type a_producer payload_producer payload)
  ~(iterator : (int, a_producer, payload, payload_producer) iterator)
  (module Payload : Message.Payload with type t = payload)
  ~init
  ~(f : _ -> a_producer -> payload_producer)
  =
  let reader ~worker =
    let reader =
      Sequence.range 0 5
      |> Sequence.map ~f:(fun i -> { Message.worker; payload = i })
      |> Pipe.of_sequence
    in
    Pipe.set_size_budget reader 1;
    reader
  in
  let (module Parallel_iterator) =
    match iterator with
    | Global ->
      Parallel_iterator.make
        (Worker.make
           ~create_producer:(fun ({ Args.worker; _ } as args) ->
             let producer = Iterator.of_pipe_reader (reader ~worker) in
             let state = init args in
             return (Ok (f state producer)))
           ~create_consumer:(fun _ writer ->
             return
               (Ok (Iterator.of_direct_stream_writer ~flush_every:Int.max_value writer)))
           ~bin_args:Args.bin_t
           ~bin_message:(Message.bin_t Payload.bin_t))
    | Batched ->
      Parallel_iterator.make
        (Worker.make
           ~create_producer:(fun ({ Args.worker; _ } as args) ->
             let producer = Iterator.Batched.of_pipe_reader (reader ~worker) in
             let state = init args in
             return (Ok (f state producer)))
           ~create_consumer:(fun _ writer ->
             return (Ok (Iterator.Batched.of_direct_stream_writer writer)))
           ~bin_args:Args.bin_t
           ~bin_message:(Message.bin_t Payload.bin_t))
  in
  let number_of_workers = 3 in
  let workers =
    List.init number_of_workers ~f:(fun (_ : int) -> (), ()) |> Nonempty_list.of_list_exn
  in
  let%bind producer =
    Iterator.create_producer_with_resource
      (fun () -> Parallel_iterator.create workers)
      ~close:(Parallel_iterator.close >> Deferred.ok)
      ~create:
        (Parallel_iterator.create_producer ~args:(fun ~worker () () ->
           { number_of_workers; worker }))
    >>| ok_exn
  in
  let consumer =
    Iterator.create_consumer
      ~f:(fun message ->
        print_s [%sexp (message : Payload.t Message.t)];
        Maybe_pushback.unit)
      ()
  in
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  Iterator.start producer consumer >>| ok_exn
;;

let run_test m_payload ~global ~batched ~init ~f ~expect =
  let%bind () =
    gen_iter_with ~iterator:Global m_payload ~init ~f:(fun state producer ->
      global producer ~f:(fun message -> f state message))
  in
  expect ();
  let%bind () =
    gen_iter_with ~iterator:Batched m_payload ~init ~f:(fun state producer ->
      batched producer ~f:(fun message -> f state message))
  in
  expect ();
  return ()
;;

let inspect ~number_of_workers { Message.worker; payload } =
  printf "worker %d/%d saw %d\n" worker number_of_workers payload
;;

let%expect_test "inspect" =
  run_test
    (module Int)
    ~global:Iterator.inspect
    ~batched:Iterator.Batched.inspect
    ~init:Args.number_of_workers
    ~f:(fun number_of_workers message -> inspect ~number_of_workers message)
    ~expect:(fun () ->
      [%expect
        {|
        worker 0/3 saw 0
        worker 0/3 saw 1
        worker 1/3 saw 0
        worker 1/3 saw 1
        worker 2/3 saw 0
        worker 2/3 saw 1
        ((worker 0) (payload 0))
        ((worker 0) (payload 1))
        ((worker 1) (payload 0))
        ((worker 1) (payload 1))
        ((worker 2) (payload 0))
        ((worker 2) (payload 1))
        worker 0/3 saw 2
        worker 0/3 saw 3
        worker 1/3 saw 2
        worker 1/3 saw 3
        worker 2/3 saw 2
        worker 2/3 saw 3
        ((worker 0) (payload 2))
        ((worker 0) (payload 3))
        ((worker 1) (payload 2))
        ((worker 1) (payload 3))
        ((worker 2) (payload 2))
        ((worker 2) (payload 3))
        worker 0/3 saw 4
        worker 1/3 saw 4
        worker 2/3 saw 4
        ((worker 0) (payload 4))
        ((worker 1) (payload 4))
        ((worker 2) (payload 4))
        |}])
;;

let filter ~number_of_workers { Message.worker; payload } =
  payload mod number_of_workers = worker
;;

let%expect_test "filter" =
  run_test
    (module Int)
    ~global:Iterator.filter
    ~batched:Iterator.Batched.filter
    ~init:Args.number_of_workers
    ~f:(fun number_of_workers message -> filter ~number_of_workers message)
    ~expect:(fun () ->
      [%expect
        {|
        ((worker 0) (payload 0))
        ((worker 1) (payload 1))
        ((worker 0) (payload 3))
        ((worker 2) (payload 2))
        ((worker 1) (payload 4))
        |}])
;;

let init_float_base args = (10. *. Float.of_int (Args.worker args + 1)) +. 0.5

let to_float ~base { Message.worker; payload } =
  { Message.worker; payload = base +. Float.of_int payload }
;;

let%expect_test "map" =
  run_test
    (module Float)
    ~global:Iterator.map
    ~batched:Iterator.Batched.map
    ~init:init_float_base
    ~f:(fun base message -> to_float ~base message)
    ~expect:(fun () ->
      [%expect
        {|
        ((worker 0) (payload 10.5))
        ((worker 0) (payload 11.5))
        ((worker 1) (payload 20.5))
        ((worker 1) (payload 21.5))
        ((worker 2) (payload 30.5))
        ((worker 2) (payload 31.5))
        ((worker 0) (payload 12.5))
        ((worker 0) (payload 13.5))
        ((worker 1) (payload 22.5))
        ((worker 1) (payload 23.5))
        ((worker 2) (payload 32.5))
        ((worker 2) (payload 33.5))
        ((worker 0) (payload 14.5))
        ((worker 1) (payload 24.5))
        ((worker 2) (payload 34.5))
        |}])
;;

let%expect_test "filter_map" =
  run_test
    (module Float)
    ~global:Iterator.filter_map
    ~batched:Iterator.Batched.filter_map
    ~init:(fun args -> Args.number_of_workers args, init_float_base args)
    ~f:(fun (number_of_workers, base) message ->
      if filter ~number_of_workers message then Some (to_float ~base message) else None)
    ~expect:(fun () ->
      [%expect
        {|
        ((worker 0) (payload 10.5))
        ((worker 1) (payload 21.5))
        ((worker 0) (payload 13.5))
        ((worker 2) (payload 32.5))
        ((worker 1) (payload 24.5))
        |}])
;;

let%expect_test "concat_map" =
  run_test
    (module String)
    ~global:Iterator.concat_map
    ~batched:Iterator.Batched.concat_map
    ~init:ignore
    ~f:(fun () { Message.worker; payload } ->
      List.init (worker + 1) ~f:(fun (_ : int) ->
        { Message.worker
        ; payload =
            String.init (worker + 1) ~f:(fun (_ : int) ->
              payload |> Int.to_string |> Char.of_string)
        }))
    ~expect:(fun () ->
      [%expect
        {|
        ((worker 0) (payload 0))
        ((worker 0) (payload 1))
        ((worker 1) (payload 00))
        ((worker 1) (payload 00))
        ((worker 1) (payload 11))
        ((worker 1) (payload 11))
        ((worker 2) (payload 000))
        ((worker 2) (payload 000))
        ((worker 2) (payload 000))
        ((worker 2) (payload 111))
        ((worker 2) (payload 111))
        ((worker 2) (payload 111))
        ((worker 0) (payload 2))
        ((worker 0) (payload 3))
        ((worker 1) (payload 22))
        ((worker 1) (payload 22))
        ((worker 1) (payload 33))
        ((worker 1) (payload 33))
        ((worker 2) (payload 222))
        ((worker 2) (payload 222))
        ((worker 2) (payload 222))
        ((worker 2) (payload 333))
        ((worker 2) (payload 333))
        ((worker 2) (payload 333))
        ((worker 0) (payload 4))
        ((worker 1) (payload 44))
        ((worker 1) (payload 44))
        ((worker 2) (payload 444))
        ((worker 2) (payload 444))
        ((worker 2) (payload 444))
        |}])
;;
