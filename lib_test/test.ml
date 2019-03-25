(*
 * Copyright (C) 2013 Citrix Inc
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
 * OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 *)
open OUnit2

module Int = struct
  type t = int
  let compare (x: t) (y: t) = Pervasives.compare x y
  let zero = 0
  let succ x = x + 1
  let pred x = x - 1
  let add x y = x + y
  let sub x y = x - y
  let to_string = string_of_int
end

module IntDiet = struct
  include Diet.Make(Int)

  let add (x, y) t =
    add (Interval.make x y) t
end

let test_printer ctxt =
  let open IntDiet in
  let t = add (1, 2) @@ add (4, 5) empty in
  let got = Format.asprintf "%a" pp t in
  let expected = String.trim {|
x: 4
y: 5
l:
  x: 1
  y: 2
  l:
    Empty
  r:
    Empty
  h: 1
  cardinal: 2
r:
  Empty
h: 2
cardinal: 4|}
  in
  assert_equal ~ctxt ~printer:(fun s -> s) ~cmp:String.equal expected got

let test_find_next_gap ctxt =
  let open IntDiet in
  let set = add (9, 9) @@ add (5, 7) empty in
  let test n ~expected =
    let got = find_next_gap n set in
    assert_equal ~ctxt ~printer:string_of_int expected got
  in
  test 0 ~expected:0;
  test 5 ~expected:8;
  test 9 ~expected:10;
  for i = 0 to 12 do
    let e = find_next_gap i set in
    assert (e >= i);
    assert (not @@ mem e set);
    assert (e == i || mem i set);
    assert (find_next_gap e set = e)
  done

let suite =
  "diet" >:::
  (
  List.map
    (fun (name, fn) -> name >:: (fun _ctx -> fn ()))
    Diet.Test.all
  @
  [ "finding the next gap" >:: test_find_next_gap
  ; "printer" >:: test_printer
  ]
  )

let () = run_test_tt_main suite
