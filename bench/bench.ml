open Core
open Core_bench.Std

module IntDiet = Diet.Make(struct
    type t = int
    let compare (x: t) (y: t) = Pervasives.compare x y
    let zero = 0
    let succ x = x + 1
    let pred x = x - 1
    let add x y = x + y
    let sub x y = x - y
    let to_string = string_of_int
  end)

(** This fix state ensures that the benchmarks are reproducible.
    Those constants have been generated using [Random.int 1_073_741_823].
*)
let state = Random.State.make [|994326685; 290180182; 366831641|]

let fisher_yates_shuffle a =
  for i = Array.length a-1 downto 1 do
    let j = Random.State.int state (i + 1) in
    let tmp = a.(i) in
    a.(i) <- a.(j);
    a.(j) <- tmp;
  done

let gen_array size =
  let gen_interval i =
    let length = Random.State.int state 8 in
    IntDiet.Interval.make (10 * i) (10 * i + length)
  in
  Array.init size ~f:gen_interval

let diet_from_array arr =
  Array.fold
    ~init:IntDiet.empty
    ~f:(fun diet intvl -> IntDiet.add intvl diet)
    arr

let gen_diets n =
  let intervals = gen_array n in
  let regular = diet_from_array intervals in
  fisher_yates_shuffle intervals;
  let shuffled = diet_from_array intervals in
  let different = diet_from_array (gen_array n) in
  regular, shuffled, different

let d10, d10', e10 = gen_diets 10
let d100, d100', e100 = gen_diets 100
let d1000, d1000', e1000 = gen_diets 1000

let () =
  Command.run
    (Bench.make_command
       [ Bench.Test.create ~name:"Equal (size 10)" (fun () -> ignore @@ IntDiet.equal d10 d10')
       ; Bench.Test.create ~name:"Equal (size 100)" (fun () -> ignore @@ IntDiet.equal d100 d100')
       ; Bench.Test.create ~name:"Equal (size 1000)" (fun () -> ignore @@ IntDiet.equal d1000 d1000')
       ; Bench.Test.create ~name:"Not equal (size 10)" (fun () -> ignore @@ IntDiet.equal d10 e10)
       ; Bench.Test.create ~name:"Not equal (size 100)" (fun () -> ignore @@ IntDiet.equal d100 e100)
       ; Bench.Test.create ~name:"Not equal (size 1000)" (fun () -> ignore @@ IntDiet.equal d1000 e1000)
       ])
