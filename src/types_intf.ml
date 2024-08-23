open! Core
open! Async
open! Import

module type S = sig
  type 'message wrapper
  type ('a, 'b) create_f
  type ('a, 'b) create'_f
  type ('a, 'b) op_f
end

module type Types = sig
  module type S = S

  module Local :
    S
    with type 'message wrapper = 'message
     and type ('a, 'b) create_f = local_ 'a -> 'b
     and type ('a, 'b) create'_f = local_ 'a -> local_ 'b
     and type ('a, 'b) op_f = local_ 'a -> local_ 'b

  module Global :
    S
    with type 'message wrapper = 'message Modes.Global.t
     and type ('a, 'b) create_f = 'a -> 'b
     and type ('a, 'b) create'_f = 'a -> local_ 'b
     and type ('a, 'b) op_f = 'a -> 'b

  module Batched :
    S
    with type 'message wrapper = 'message Queue.t Modes.Global.t
     and type ('a, 'b) create_f = 'a Queue.t -> 'b
     and type ('a, 'b) create'_f = 'a Queue.t -> local_ 'b
     and type ('a, 'b) op_f = 'a -> 'b

  (** Certain types are erased from each iterator interface, as they are mostly an 
      artifact of (the current lack of) mode polymorphism, and would obfuscate the type 
      signatures produced by Merlin. 

      This functor can be used to recover those types, allowing code to be functorized 
      over different kinds of iterator. *)
  module Unerase
      (Types : S)
      (Erased : Iterator.S
                with type 'message wrapper := 'message Types.wrapper
                 and type ('a, 'b) create_f := ('a, 'b) Types.create_f
                 and type ('a, 'b) create'_f := ('a, 'b) Types.create'_f
                 and type ('a, 'b) op_f := ('a, 'b) Types.op_f) :
    Iterator.S
    with type 'message wrapper = 'message Types.wrapper
     and type ('a, 'b) create_f = ('a, 'b) Types.create_f
     and type ('a, 'b) create'_f = ('a, 'b) Types.create'_f
     and type ('a, 'b) op_f = ('a, 'b) Types.op_f
end
