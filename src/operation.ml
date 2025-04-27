open! Core
open! Async
open! Import

[%%template
[@@@mode.default m = (global, local)]

type ('a, 'b) t =
  | Ident : (('a, 'a) t[@mode m])
  | Inspect : ('a @ m -> unit) -> (('a, 'a) t[@mode m])
  | Filter : ('a @ m -> bool) -> (('a, 'a) t[@mode m])
  | Map : ('a @ m -> 'b @ m) -> (('a, 'b) t[@mode m])
  | Filter_map : ('a @ m -> 'b option @ m) -> (('a, 'b) t[@mode m])
  | Concat_map : ('a @ m -> 'b list @ m) -> (('a, 'b) t[@mode m])

let apply
  (type a b)
  (t : ((a, b) t[@mode m]))
  (producer : (a Iterator.Producer.t[@mode m]))
  : (b Iterator.Producer.t[@mode m])
  =
  match t with
  | Ident -> producer
  | Inspect f -> (Iterator.inspect [@mode m]) producer ~f
  | Filter f -> (Iterator.filter [@mode m]) producer ~f
  | Map f -> (Iterator.map [@mode m]) producer ~f
  | Filter_map f -> (Iterator.filter_map [@mode m]) producer ~f
  | Concat_map f -> (Iterator.concat_map [@mode m]) producer ~f
;;

let contra_apply
  (type a b)
  (t : ((a, b) t[@mode m]))
  (consumer : (b Iterator.Consumer.t[@mode m]))
  : (a Iterator.Consumer.t[@mode m])
  =
  match t with
  | Ident -> consumer
  | Inspect f -> (Iterator.contra_inspect [@mode m]) consumer ~f
  | Filter f -> (Iterator.contra_filter [@mode m]) consumer ~f
  | Map f -> (Iterator.contra_map [@mode m]) consumer ~f
  | Filter_map f -> (Iterator.contra_filter_map [@mode m]) consumer ~f
  | Concat_map f -> (Iterator.contra_concat_map [@mode m]) consumer ~f
;;]

module Batched = struct
  let apply (type a b) (t : (a, b) t) (producer : a Iterator.Batched.Producer.t)
    : b Iterator.Batched.Producer.t
    =
    match t with
    | Ident -> producer
    | Inspect f -> Iterator.Batched.inspect producer ~f
    | Filter f -> Iterator.Batched.filter producer ~f
    | Map f -> Iterator.Batched.map producer ~f
    | Filter_map f -> Iterator.Batched.filter_map producer ~f
    | Concat_map f -> Iterator.Batched.concat_map producer ~f
  ;;

  let contra_apply (type a b) (t : (a, b) t) (consumer : b Iterator.Batched.Consumer.t)
    : a Iterator.Batched.Consumer.t
    =
    match t with
    | Ident -> consumer
    | Inspect f -> Iterator.Batched.contra_inspect consumer ~f
    | Filter f -> Iterator.Batched.contra_filter consumer ~f
    | Map f -> Iterator.Batched.contra_map consumer ~f
    | Filter_map f -> Iterator.Batched.contra_filter_map consumer ~f
    | Concat_map f -> Iterator.Batched.contra_concat_map consumer ~f
  ;;
end
