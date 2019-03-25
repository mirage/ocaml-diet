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

  let remove (x, y) t =
    remove (Interval.make x y) t

  let elements t = fold_individual (fun x acc -> x :: acc) t [] |> List.rev
end

module IntSet = Set.Make(Int)

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

let make_random n m =
  let rec loop set diet = function
    | 0 -> set, diet
    | m ->
      let r = Random.int n in
      let r' = Random.int (n - r) + r in
      let add = Random.bool () in
      let rec range from upto =
        if from > upto then [] else from :: (range (from + 1) upto) in
      let set = List.fold_left (fun set elt -> (if add then IntSet.add else IntSet.remove) elt set) set (range r r') in
      let diet' = (if add then IntDiet.add else IntDiet.remove) (r, r') diet in
      begin
        try
          IntDiet.check_invariants diet'
        with e ->
          Format.eprintf "%s %d\nBefore: %a\nAfter: %a\n"
            (if add then "Add" else "Remove") r
            IntDiet.pp diet IntDiet.pp diet';
          raise e
      end;
      loop set diet' (m - 1) in
  loop IntSet.empty IntDiet.empty m

let show_list show l =
  Printf.sprintf "[%s]" (String.concat "; " (List.map show l))

let check_equals ?msg ~ctxt set diet =
  let set' = IntSet.elements set in
  let diet' = IntDiet.elements diet in
  let printer = show_list string_of_int in
  assert_equal ?msg ~ctxt ~printer set' diet'

let test_operators ops ctxt =
  for _ = 1 to 100 do
    let set1, diet1 = make_random 1000 1000 in
    let set2, diet2 = make_random 1000 1000 in
    check_equals ~ctxt set1 diet1;
    List.iter (fun (op_name, set_op, diet_op) ->
      let msg = "When checking " ^ op_name in
      let set3 = set_op set1 set2 in
      let diet3 = diet_op diet1 diet2 in
      check_equals ~msg ~ctxt set3 diet3
      ) ops
  done

let test_depth ctxt =
  let n = 0x100000 in
  let init = IntDiet.add (0, n) IntDiet.empty in
  (* take away every other block *)
  let rec sub m acc =
    if m <= 0 then acc
    else sub (m - 2) (IntDiet.remove (m, m) acc) in
  let set = sub n init in
  let d = IntDiet.height set in
  let bound = int_of_float (log (float_of_int n) /. (log 2.)) + 1 in
  assert_bool "Depth lower than bound" (d <= bound);
  let set = sub (n - 1) set in
  let got = IntDiet.height set in
  let expected = 1 in
  assert_equal ~ctxt ~printer:string_of_int expected got

let suite =
  "diet" >:::
  (
  List.map
    (fun (name, fn) -> name >:: (fun _ctx -> fn ()))
    Diet.Test.all
  @
  [ "logarithmic depth" >:: test_depth
  ; "operators" >:: test_operators
    [ ("union", IntSet.union, IntDiet.union)
    ; ("diff", IntSet.diff, IntDiet.diff)
    ; ("intersection", IntSet.inter, IntDiet.inter)
    ]
  ; "finding the next gap" >:: test_find_next_gap
  ; "printer" >:: test_printer
  ]
  )

let () = run_test_tt_main suite
