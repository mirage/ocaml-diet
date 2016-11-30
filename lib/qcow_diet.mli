(*
 * Copyright (C) 2016 David Scott <dave@recoil.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *)

module type ELT = sig
  type t [@@deriving sexp]
  (** The type of the set elements. *)

  include Set.OrderedType with type t := t

  val pred: t -> t
  (** Predecessor of an element *)

  val succ: t -> t
  (** Successor of an element *)
end

module Make(Elt: ELT): sig
  (** A discrete interval encoding tree: a set of elements optimised for cases
      where large numbers of contiguous elements are in the set. *)

  type elt = Elt.t [@@deriving sexp]
  (** The type of the set elements *)

  type interval = elt * elt
  (** An interval: a range (x, y) of set values where all the elements from
      x to y inclusive are in the set *)

  module Interval: sig
    val make: elt -> elt -> interval
    (** [make first last] construct an interval describing all the elements from
        [first] to [last] inclusive. *)
  end

  type t [@@deriving sexp]
  (** The type of sets *)

  val empty: t
  (** The empty set *)

  val is_empty: t -> bool
  (** Test whether a set is empty or not *)

  val mem: elt -> t -> bool
  (** [mem elt t] tests whether [elt] is in set [t] *)

  val fold: (interval -> 'a -> 'a) -> t -> 'a -> 'a
  (** [fold f t acc] folds [f] across all the intervals in [t] *)

  val add: interval -> t -> t
  (** [add interval t] returns the set consisting of [t] plus [interval] *)

  val remove: interval -> t -> t
  (** [remove interval t] returns the set consisting of [t] minus [interval] *)

  val max_elt: t -> interval
  (** [max_elt t] returns the largest (in terms of the ordering) interval in
      [t], or raises [Not_found] if the set is empty. *)

  val union: t -> t -> t
  (** set union *)

  val diff: t -> t -> t
  (** set difference *)

  val inter: t -> t -> t
  (** set intersection *)
end

module Test: sig
  val all: (string * (unit -> unit)) list
end
