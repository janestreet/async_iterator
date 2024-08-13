open! Core
open! Async
open! Import
include Types_intf

module Local = struct
  type 'message wrapper = 'message
  type ('a, 'b) create_f = 'a -> 'b
  type ('a, 'b) create'_f = 'a -> 'b
  type ('a, 'b) op_f = 'a -> 'b
end

module Global = struct
  type 'message wrapper = 'message Modes.Global.t
  type ('a, 'b) create_f = 'a -> 'b
  type ('a, 'b) create'_f = 'a -> 'b
  type ('a, 'b) op_f = 'a -> 'b
end

module Batched = struct
  type 'message wrapper = 'message Queue.t Modes.Global.t
  type ('a, 'b) create_f = 'a Queue.t -> 'b
  type ('a, 'b) create'_f = 'a Queue.t -> 'b
  type ('a, 'b) op_f = 'a -> 'b
end

module Unerase
    (Types : S)
    (Erased : Iterator.S
              with type 'message wrapper := 'message Types.wrapper
               and type ('a, 'b) create_f := ('a, 'b) Types.create_f
               and type ('a, 'b) create'_f := ('a, 'b) Types.create'_f
               and type ('a, 'b) op_f := ('a, 'b) Types.op_f) =
struct
  include Types
  include Erased
end
