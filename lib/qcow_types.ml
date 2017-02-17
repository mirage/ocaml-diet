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
open Sexplib.Std
open Qcow_error

let big_enough_for name buf needed =
  let length = Cstruct.len buf in
  if length < needed
  then error_msg "%s: buffer too small (%d < %d)" name length needed
  else return ()

module Int8 = struct
  type t = int [@@deriving sexp]

  let sizeof _ = 1

  let read buf =
    big_enough_for "Int8.read" buf 1
    >>= fun () ->
    return (Cstruct.get_uint8 buf 0, Cstruct.shift buf 1)

  let write t buf =
    big_enough_for "Int8.write" buf 1
    >>= fun () ->
    Cstruct.set_uint8 buf 0 t;
    return (Cstruct.shift buf 1)
end

module Int16 = struct
  type t = int [@@deriving sexp]

  let sizeof _ = 2

  let read buf =
    big_enough_for "Int16.read" buf 2
    >>= fun () ->
    return (Cstruct.BE.get_uint16 buf 0, Cstruct.shift buf 2)

  let write t buf =
    big_enough_for "Int16.write" buf 2
    >>= fun () ->
    Cstruct.BE.set_uint16 buf 0 t;
    return (Cstruct.shift buf 2)
end

module Int32 = struct
  include Int32

  type _t = int32 [@@deriving sexp]
  let sexp_of_t = sexp_of__t
  let t_of_sexp = _t_of_sexp

  let sizeof _ = 4

  let read buf =
    big_enough_for "Int32.read" buf 4
    >>= fun () ->
    return (Cstruct.BE.get_uint32 buf 0, Cstruct.shift buf 4)

  let write t buf =
    big_enough_for "Int32.read" buf 4
    >>= fun () ->
    Cstruct.BE.set_uint32 buf 0 t;
    return (Cstruct.shift buf 4)
end

module Int64 = struct
  module M = struct
    include Int64

    type _t = int64 [@@deriving sexp]
    let sexp_of_t = sexp_of__t
    let t_of_sexp = _t_of_sexp

    let to_int64 x = x
    let of_int64 x = x
  end
  module IntervalSet = Qcow_diet.Make(M)
  module Map = Map.Make(M)
  include M

  let round_up x size = mul (div (add x (pred size)) size) size

  let sizeof _ = 8

  let read buf =
    big_enough_for "Int64.read" buf 8
    >>= fun () ->
    return (Cstruct.BE.get_uint64 buf 0, Cstruct.shift buf 8)

  let write t buf =
    big_enough_for "Int64.read" buf 8
    >>= fun () ->
    Cstruct.BE.set_uint64 buf 0 t;
    return (Cstruct.shift buf 8)

end

module Int = struct
  module M = struct
    type t = int [@@deriving sexp]
    let zero = 0
    let succ x = x + 1
    let pred x = x - 1
    let add x y = x + y
    let sub x y = x - y
    let compare (x: t) (y: t) = Pervasives.compare x y
    let mul x y = x * y
    let div x y = x / y
    let to_int64 = Int64.of_int
    let of_int64 = Int64.to_int
  end
  module IntervalSet = Qcow_diet.Make(M)
  module Map = Map.Make(M)
  include M

  let round_up x size = mul (div (add x (pred size)) size) size

end

module Cluster = Int64
