open! Core
open! Async
open! Import

type ('config, 'info, 'args, 'message) t =
  | T :
      (module Worker.S
         with type config = 'config
          and type args = 'args
          and type message = 'message
          and type t = 'worker)
      * ('config * 'info * 'worker) array
      -> ('config, 'info, 'args, 'message) t

let create
  (type config args message)
  (module Worker : Worker.S
    with type config = config
     and type args = args
     and type message = message)
  workers
  =
  let%bind workers, errors =
    Deferred.List.map
      ~how:`Parallel
      (Nonempty_list.to_list workers)
      ~f:(fun (config, info) ->
        let%bind.Deferred.Or_error worker = Worker.create config in
        return (Ok (config, info, worker)))
    >>| List.partition_result
  in
  if List.is_empty errors
  then return (Ok (T ((module Worker), Array.of_list workers)))
  else (
    let%bind () = Deferred.List.iter ~how:`Parallel workers ~f:(trd3 >> Worker.close) in
    return (Error (Error.of_list errors)))
;;

let close
  (type config args message)
  (T ((module Worker), workers) : (config, _, args, message) t)
  =
  Deferred.Array.iter ~how:`Parallel workers ~f:(trd3 >> Worker.close)
;;

let create_producer
  (type config args message)
  (T ((module Worker), workers) : (config, _, args, message) t)
  ~args
  =
  let tag result ~worker = Or_error.tag_arg result "worker" worker [%sexp_of: int] in
  Iterator.Global.create_producer_staged'
    ~iter:(fun ~f ->
      let%bind producers, errors =
        Deferred.List.mapi
          ~how:`Parallel
          (Array.to_list workers)
          ~f:(fun i (config, info, worker) ->
            Worker.create_producer worker (args ~worker:i config info) >>| tag ~worker:i)
        >>| List.partition_result
      in
      if List.is_empty errors
      then return (Ok (producers, Iterator.Global.create_consumer' ~f ()))
      else (
        let%bind (_ : unit Or_error.t list) =
          Deferred.List.map ~how:`Parallel producers ~f:Iterator.abort
        in
        return (Error (Error.of_list errors))))
    ~start:(fun (producers, consumer) ~stop ->
      let any_error = Ivar.create () in
      let consumer =
        consumer
        |> Iterator.add_stop' ~stop
        |> Iterator.add_stop ~stop:(Ivar.read any_error)
      in
      List.mapi producers ~f:(fun i producer ->
        let%map stopped = Iterator.start producer consumer in
        if Result.is_error stopped then Ivar.fill_if_empty any_error ();
        tag stopped ~worker:i)
      |> Deferred.Or_error.combine_errors_unit)
;;

let number_of_workers (T (_, workers)) = Array.length workers
