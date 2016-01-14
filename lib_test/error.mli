(*
 * Copyright (C) 2016 David Scott <dave.scott@unikernel.com>
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

type 'a error = [ `Ok of 'a | `Error of [ `Msg of string ] ]

module FromBlock: sig
  val ( >>= ): [< `Error of Mirage_block.Error.error | `Ok of 'a ] Lwt.t
    -> ('a -> ([> `Error of [> `Msg of bytes ] ] as 'b) Lwt.t)
    -> 'b Lwt.t
end

module Infix: sig
  val ( >>= ) : [< `Error of 'a | `Ok of 'b ] Lwt.t
    -> ('b -> ([> `Error of 'a ] as 'c) Lwt.t)
    -> 'c Lwt.t
end

module FromResult: sig
  val ( >>= ) :   ('a, 'b) Result.result -> ('a -> ([> `Error of 'b ] as 'c) Lwt.t) -> 'c Lwt.t

end
