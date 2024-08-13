open! Core
open! Async
open! Import

module type S = sig
  type ('a, 'b) f
  type 'message producer
  type 'message consumer

  type ('a, 'b) t =
    | Ident : ('a, 'a) t
    | Inspect : ('a, unit) f -> ('a, 'a) t
    | Filter : ('a, bool) f -> ('a, 'a) t
    | Map : ('a, 'b) f -> ('a, 'b) t
    | Filter_map : ('a, 'b option) f -> ('a, 'b) t
    | Concat_map : ('a, 'b list) f -> ('a, 'b) t

  val apply : ('a, 'b) t -> producer:'a producer -> 'b producer
  val contra_apply : ('a, 'b) t -> consumer:'b consumer -> 'a consumer
end

module type Operation = sig
  module type S = S

  (** This functor can be used to produce a variant describing different kinds of
      operations applicable to a particular iterator type. *)
  module Make (Iterator : Iterator.S) :
    S
    with type ('a, 'b) f := ('a, 'b) Iterator.op_f
     and type 'message producer := 'message Iterator.Producer.t
     and type 'message consumer := 'message Iterator.Consumer.t
end
