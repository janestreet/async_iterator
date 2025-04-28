open! Core
open! Async
open! Import

[%%template:
[@@@mode.default m = (global, local)]

(** This variant describes the kinds of operations applicable to iterator types. *)
type ('a, 'b) t =
  | Ident : (('a, 'a) t[@mode m])
  | Inspect : ('a -> unit) -> (('a, 'a) t[@mode m])
  | Filter : ('a -> bool) -> (('a, 'a) t[@mode m])
  | Map : ('a -> 'b) -> (('a, 'b) t[@mode m])
  | Filter_map : ('a -> 'b option) -> (('a, 'b) t[@mode m])
  | Concat_map : ('a -> 'b list) -> (('a, 'b) t[@mode m])

val apply
  :  (('a, 'b) t[@mode m])
  -> ('a Iterator.Producer.t[@mode m])
  -> ('b Iterator.Producer.t[@mode m])

val contra_apply
  :  (('a, 'b) t[@mode m])
  -> ('b Iterator.Consumer.t[@mode m])
  -> ('a Iterator.Consumer.t[@mode m])]

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
