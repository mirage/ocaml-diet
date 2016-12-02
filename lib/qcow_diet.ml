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
(* #require "ppx_sexp_conv";; *)
open Sexplib.Std

module type ELT = sig
  type t [@@deriving sexp]
  val compare: t -> t -> int
  val pred: t -> t
  val succ: t -> t
end

exception Interval_pairs_should_be_ordered of string
exception Intervals_should_not_overlap of string

let _ =
  Printexc.register_printer
    (function
      | Interval_pairs_should_be_ordered txt ->
        Some ("Pairs within each interval should be ordered: " ^ txt)
      | Intervals_should_not_overlap txt ->
        Some ("Intervals should be ordered without overlap: " ^ txt)
      | _ ->
        None
    )

module Make(Elt: ELT) = struct
  type elt = Elt.t [@@deriving sexp]

  type interval = elt * elt

  module Interval = struct
    let make x y =
      if x > y then invalid_arg "Interval.make";
      x, y
  end

  let ( >  ) x y = Elt.compare x y > 0
  let ( >= ) x y = Elt.compare x y >= 0
  let ( <  ) x y = Elt.compare x y < 0
  let ( <= ) x y = Elt.compare x y <= 0
  let eq     x y = Elt.compare x y = 0
  let succ, pred = Elt.succ, Elt.pred

  type t =
    | Empty
    | Node: node -> t
  and node = { x: elt; y: elt; l: t; r: t }
  [@@deriving sexp]

  let to_string_internal t = Sexplib.Sexp.to_string_hum @@ sexp_of_t t

  module Invariant = struct

    (* The pairs (x, y) in each interval are ordered such that x <= y *)
    let rec ordered t = match t with
      | Empty -> ()
      | Node { x; y; l; r } ->
        if x > y then raise (Interval_pairs_should_be_ordered (to_string_internal t));
        ordered l;
        ordered r

    (* The intervals don't overlap *)
    let rec no_overlap t = match t with
      | Empty -> ()
      | Node { x; y; l; r } ->
        begin match l with
          | Empty -> ()
          | Node left -> if left.y >= x then raise (Intervals_should_not_overlap (to_string_internal t))
        end;
        begin match r with
          | Empty -> ()
          | Node right -> if right.x <= y then raise (Intervals_should_not_overlap (to_string_internal t))
        end;
        no_overlap l;
        no_overlap r

    let check t =
      ordered t;
      no_overlap t
  end

  let empty = Empty

  let is_empty = function
    | Empty -> true
    | _ -> false

  let rec mem elt = function
    | Empty -> false
    | Node n ->
      (* consider this interval *)
      (elt >= n.x && elt <= n.y)
      ||
      (* or search left or search right *)
      (if elt < n.x then mem elt n.l else mem elt n.r)

  let rec min_elt = function
    | Empty  -> raise Not_found
    | Node { x; y; l = Empty; _ } -> x, y
    | Node { l; _ } -> min_elt l

  let rec max_elt = function
    | Empty -> raise Not_found
    | Node { x; y; r = Empty; _ } -> x, y
    | Node { r; _ } -> max_elt r

  let choose = function
    | Empty -> raise Not_found
    | Node { x; y; _ } -> x, y

  (* fold over the maximal contiguous intervals *)
  let rec fold f t acc = match t with
    | Empty -> acc
    | Node n ->
      let acc = fold f n.l acc in
      let acc = f (n.x, n.y) acc in
      fold f n.r acc

  (* fold over individual elements *)
  let fold_individual f t acc =
    let range (from, upto) acc =
      let rec loop acc x =
        if eq x (succ upto) then acc else loop (f x acc) (succ x) in
      loop acc from in
    fold range t acc

  let elements t = fold_individual (fun x acc -> x :: acc) t [] |> List.rev

  (* return (x, y, l) where (x, y) is the maximal interval and [l] is
     the rest of the tree on the left (whose intervals are all smaller). *)
  let rec splitMax = function
    | { x; y; l; r = Empty } -> x, y, l
    | { r = Node r; _ } as n ->
      let u, v, r' = splitMax r in
      u, v, Node { n with r = r' }

  (* return (x, y, r) where (x, y) is the minimal interval and [r] is
     the rest of the tree on the right (whose intervals are all larger) *)
  let rec splitMin = function
    | { x; y; l = Empty; r } -> x, y, r
    | { l = Node l; _ } as n ->
      let u, v, l' = splitMin l in
      u, v, Node { n with l = l' }

  let addL = function
    | { l = Empty; _ } as n -> n
    | { l = Node l; _ } as n ->
      (* we might have to merge the new element with the maximal interval from
         the left *)
      let x', y', l' = splitMax l in
      if eq (succ y') n.x then { n with x = x'; l = l' } else n

  let addR = function
    | { r = Empty; _ } as n -> n
    | { r = Node r; _ } as n ->
      (* we might have to merge the new element with the minimal interval on
         the right *)
      let x', y', r' = splitMin r in
      if eq (succ n.y) x' then { n with y = y'; r = r' } else n

  let rec add (x, y) t = match t with
    | Empty -> Node { x; y; l = Empty; r = Empty }
    (* completely to the left *)
    | Node n when y < n.x ->
      let l = add (x, y) n.l in
      Node (addL { n with l })
    (* completely to the right *)
    | Node n when n.y < x ->
      let r = add (x, y) n.r in
      Node (addR { n with r })
    (* overlap on the left only *)
    | Node n when x < n.x && y <= n.y ->
      let l = add (x, pred n.x) n.l in
      Node (addL { n with l })
    (* overlap on the right only *)
    | Node n when y > n.y && x >= n.x ->
      let r = add (succ n.y, y) n.r in
      Node (addR { n with r })
    (* overlap on both sides *)
    | Node n when x < n.x && y > n.y ->
      let l = add (x, pred n.x) n.l in
      let r = add (succ n.y, y) n.r in
      Node (addL { (addR { n with r }) with l })
    (* completely within *)
    | Node n -> Node n

  let union = fold add

  let merge l r = match l, r with
    | l, Empty -> l
    | Empty, r -> r
    | Node l, r ->
      let x, y, l' = splitMax l in
      Node { x; y; l = l'; r }

  let rec remove (x, y) t = match t with
    | Empty -> Empty
    (* completely to the left *)
    | Node n when y < n.x -> Node { n with l = remove (x, y) n.l }
    (* completely to the right *)
    | Node n when n.y < x -> Node { n with r = remove (x, y) n.r }
    (* overlap on the left only *)
    | Node n when x < n.x && y < n.y ->
      remove (x, pred n.x) (Node { n with x = succ y })
    (* overlap on the right only *)
    | Node n when y > n.y && x > n.x ->
      remove (succ n.y, y) (Node { n with y = pred x })
    (* overlap on both sides *)
    | Node n when x <= n.x && y >= n.y ->
      let l = remove (x, n.x) n.l in
      let r = remove (n.y, y) n.r in
      merge l r
    (* completely within *)
    | Node n when eq y n.y -> Node { n with y = pred x }
    | Node n when eq x n.x -> Node { n with x = succ y }
    | Node n ->
      assert (n.x <= pred x);
      assert (succ y <= n.y);
      Node { n with y = (pred x); r = Node { n with x = succ y; l = Empty } }

  let diff a b = fold remove b a

  let inter a b = diff a (diff a b)

end

module Int = struct
  type t = int [@@deriving sexp]
  let compare (x: t) (y: t) = Pervasives.compare x y
  let succ x = x + 1
  let pred x = x - 1
end
module IntDiet = Make(Int)
module IntSet = Set.Make(Int)

module Test = struct

  let make_random n m =
    let rec loop set diet = function
      | 0 -> set, diet
      | m ->
        let r = Random.int n in
        let set, diet =
          if Random.bool ()
          then IntSet.add r set, IntDiet.add (IntDiet.Interval.make r r) diet
          else IntSet.remove r set, IntDiet.remove (IntDiet.Interval.make r r) diet in
        loop set diet (m - 1) in
    loop IntSet.empty IntDiet.empty m

  let set_to_string set =
    String.concat "; " @@ List.map string_of_int @@ IntSet.elements set
  let diet_to_string diet =
    String.concat "; " @@ List.map string_of_int @@ IntDiet.elements diet

  let check_equals set diet =
    let set' = IntSet.elements set in
    let diet' = IntDiet.elements diet in
    if set' <> diet' then begin
      (*
      Printf.fprintf stderr "Set contains: [ %s ]\n" @@ set_to_string set;
      Printf.fprintf stderr "Diet contains: [ %s ]\n" @@ diet_to_string diet;
      *)
      failwith "check_equals"
    end

  let test_adds () =
    for i = 1 to 1000 do
      let set, diet = make_random 1000 1000 in
      begin
        try
          IntDiet.Invariant.check diet
        with e ->
          (*
          Printf.fprintf stderr "Diet contains: [ %s ]\n" @@ IntDiet.to_string_internal diet;
          *)
          raise e
      end;
      check_equals set diet;
    done

  let test_operator set_op diet_op () =
    for i = 1 to 1000 do
      let set1, diet1 = make_random 1000 1000 in
      let set2, diet2 = make_random 1000 1000 in
      check_equals set1 diet1;
      check_equals set2 diet2;
      let set3 = set_op set1 set2 in
      let diet3 = diet_op diet1 diet2 in
      (*
      Printf.fprintf stderr "diet1 = %s\n" (IntDiet.to_string_internal diet1);
      Printf.fprintf stderr "diet3 = %s\n" (IntDiet.to_string_internal diet2);
      Printf.fprintf stderr "diet2 = %s\n" (IntDiet.to_string_internal diet3);
      *)
      check_equals set3 diet3
    done

  let test_add_1 () =
    let open IntDiet in
    assert (elements @@ add (3, 4) @@ add (3, 3) empty = [ 3; 4 ])

  let test_remove_1 () =
    let open IntDiet in
    assert (elements @@ remove (6, 7) @@ add (7, 8) empty = [ 8 ])

  let test_remove_2 () =
    let open IntDiet in
    assert (elements @@ diff (add (9, 9) @@ add (5, 7) empty) (add (7, 9) empty) = [5; 6])

  let all = [
    "adding an element to the right", test_add_1;
    "removing an element on the left", test_remove_1;
    "removing an elements from two intervals", test_remove_2;
    "adding and removing elements acts like a Set", test_adds;
    "union", test_operator IntSet.union IntDiet.union;
    "diff", test_operator IntSet.diff IntDiet.diff;
    "intersection", test_operator IntSet.inter IntDiet.inter;
  ]

end
