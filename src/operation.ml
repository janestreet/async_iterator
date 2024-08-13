open! Core
open! Async
open! Import
include Operation_intf

module Make (Iterator : Iterator.S) = struct
  type ('a, 'b) t =
    | Ident : ('a, 'a) t
    | Inspect : ('a, unit) Iterator.op_f -> ('a, 'a) t
    | Filter : ('a, bool) Iterator.op_f -> ('a, 'a) t
    | Map : ('a, 'b) Iterator.op_f -> ('a, 'b) t
    | Filter_map : ('a, 'b option) Iterator.op_f -> ('a, 'b) t
    | Concat_map : ('a, 'b list) Iterator.op_f -> ('a, 'b) t

  let apply (type a b) (t : (a, b) t) ~(producer : a Iterator.Producer.t)
    : b Iterator.Producer.t
    =
    match t with
    | Ident -> producer
    | Inspect f -> Iterator.inspect producer ~f
    | Filter f -> Iterator.filter producer ~f
    | Map f -> Iterator.map producer ~f
    | Filter_map f -> Iterator.filter_map producer ~f
    | Concat_map f -> Iterator.concat_map producer ~f
  ;;

  let contra_apply (type a b) (t : (a, b) t) ~(consumer : b Iterator.Consumer.t)
    : a Iterator.Consumer.t
    =
    match t with
    | Ident -> consumer
    | Inspect f -> Iterator.contra_inspect consumer ~f
    | Filter f -> Iterator.contra_filter consumer ~f
    | Map f -> Iterator.contra_map consumer ~f
    | Filter_map f -> Iterator.contra_filter_map consumer ~f
    | Concat_map f -> Iterator.contra_concat_map consumer ~f
  ;;
end
