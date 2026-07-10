open! Core
open! Async

type ('state, 'state_update) t =
  | Not_using : (_, Nothing.t) t
  | Using :
      { update_worker_state : 'state -> 'state_update -> unit Or_error.t Deferred.t
      ; bin_state_update : 'state_update Bin_prot.Type_class.t
      }
      -> ('state, 'state_update) t

let resolve (type state state_update) (t : (state, state_update) t) =
  match t with
  | Using { update_worker_state; bin_state_update } ->
    update_worker_state, bin_state_update
  | Not_using ->
    (fun (_ : state) state_update -> Nothing.unreachable_code state_update), Nothing.bin_t
;;
