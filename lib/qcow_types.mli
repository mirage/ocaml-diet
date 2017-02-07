(*
 * Copyright (C) 2015 David Scott <dave@recoil.org>
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
(** Parsers and printers for types used in qcow2 fields *)

open Sexplib

val big_enough_for: string -> Cstruct.t -> int -> unit Qcow_error.t
(** [big_enough_for name buf length] returns an error with a log message
    if buffer [buf] is smaller than [length]. The [name] will be included
    in the error message. *)

module Int8 : sig
  type t = int [@@deriving sexp]

  include Qcow_s.SERIALISABLE with type t := t
end

module Int16 : sig
  type t = int [@@deriving sexp]

  include Qcow_s.SERIALISABLE with type t := t
end

module Int32 : sig
  include module type of Int32

  val t_of_sexp: Sexp.t -> t
  val sexp_of_t: t -> Sexp.t

  include Qcow_s.SERIALISABLE with type t := t
end

module Int64 : sig
  include module type of Int64

  val t_of_sexp: Sexp.t -> t
  val sexp_of_t: t -> Sexp.t

  val round_up: int64 -> int64 -> int64
  (** [round_up value to] rounds [value] to the next multiple of [to] *)

  module IntervalSet: Qcow_s.INTERVAL_SET with type elt = t

  include Qcow_s.SERIALISABLE with type t := t
end
