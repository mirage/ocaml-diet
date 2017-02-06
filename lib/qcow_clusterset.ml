(*
 * Copyright (C) 2017 Docker Inc
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

include Qcow_diet.Make(Qcow_types.Int64)

let take t n =
  let rec loop acc free n =
    if n = 0L
    then Some (acc, free)
    else begin
      match (
        try
          let i = min_elt free in
          let x, y = Interval.(x i, y i) in
          let len = Int64.(succ @@ sub y x) in
          let will_use = min n len in
          let i' = Interval.make x Int64.(pred @@ add x will_use) in
          Some ((add i' acc), (remove i' free), Int64.(sub n will_use))
        with
        | Not_found -> None
      ) with
      | Some (acc', free', n') -> loop acc' free' n'
      | None -> None
    end in
  loop empty t n

let cardinal t =
  fold
    (fun i acc ->
      let from = Interval.x i in
      let upto = Interval.y i in
      let size = Int64.succ (Int64.sub upto from) in
      Int64.add size acc
    ) t 0L
