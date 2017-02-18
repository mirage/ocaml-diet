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
open Result

type error = [
  | `Msg of string
]

type 'a t = ('a, error) result

let return x = Ok x

let error_msg fmt = Printf.ksprintf (fun s -> Error (`Msg s)) fmt

let ( >>= ) m f = match m with
  | Error x -> Error x
  | Ok x -> f x

let rec any = function
  | [] -> Ok ()
  | (Error e) :: _ -> Error e
  | _ :: rest -> any rest

module Lwt_error = struct
  open Lwt.Infix
  module Infix = struct
    let ( >>= ) m f = m >>= function
      | Ok x -> f x
      | Error (`Msg s) -> Lwt.return (Error (`Msg s))
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
  end

  let or_fail_with m =
    let open Lwt in
    m >>= function
    | Error (`Msg s) -> Lwt.fail_with s
    | Error `Unimplemented -> Lwt.fail_with "unimplemented"
    | Error `Disconnected -> Lwt.fail_with "disconnected"
    | Ok x -> Lwt.return x
end

module Lwt_write_error = struct
  module Infix = struct
    open Lwt.Infix
    let ( >>= ) m f = m >>= function
      | Ok x -> f x
      | Error (`Msg s) -> Lwt.return (Error (`Msg s))
      | Error `Is_read_only -> Lwt.return (Error `Is_read_only)
      | Error `Unimplemented -> Lwt.return (Error `Unimplemented)
      | Error `Disconnected -> Lwt.return (Error `Disconnected)
  end
  let or_fail_with m =
    let open Lwt in
    m >>= function
    | Error (`Msg s) -> Lwt.fail_with s
    | Error `Unimplemented -> Lwt.fail_with "unimplemented"
    | Error `Is_read_only -> Lwt.fail_with "is read only"
    | Error `Disconnected -> Lwt.fail_with "disconnected"
    | Ok x -> Lwt.return x
end
