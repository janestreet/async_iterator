open! Core
open! Async
open! Import

[%%template:
[@@@mode m = (global, local)]

(** This variant describes the kinds of operations applicable to iterator types. *)
type ('a, 'b) t =
  | Ident : (('a, 'a) t[@mode m])
  | Inspect : ('a @ m -> unit) -> (('a, 'a) t[@mode m])
  | Filter : ('a @ m -> bool) -> (('a, 'a) t[@mode m])
  | Map : ('a @ m -> 'b @ m) -> (('a, 'b) t[@mode m])
  | Filter_map : ('a @ m -> 'b option @ m) -> (('a, 'b) t[@mode m])
  | Concat_map : ('a @ m -> 'b list @ m) -> (('a, 'b) t[@mode m])
[@@mode m]

val apply
  :  (('a, 'b) t[@mode m])
  -> ('a Iterator.Producer.t[@mode m])
  -> ('b Iterator.Producer.t[@mode m])
[@@mode m]

val contra_apply
  :  (('a, 'b) t[@mode m])
  -> ('b Iterator.Consumer.t[@mode m])
  -> ('a Iterator.Consumer.t[@mode m])
[@@mode m]]

module Batched : sig
  val apply
    :  ('a, 'b) t
    -> 'a Iterator.Batched.Producer.t
    -> 'b Iterator.Batched.Producer.t

  val contra_apply
    :  ('a, 'b) t
    -> 'b Iterator.Batched.Consumer.t
    -> 'a Iterator.Batched.Consumer.t
end
