
.PHONY: build clean test fuzz doc

build:
	dune build

test:
	dune runtest --force

fuzz:
	dune build fuzz/fuzz.exe
	mkdir -p test/input
	echo abcd > test/input/case
	afl-fuzz -i test/input -o output ./_build/default/fuzz/fuzz.exe @@

doc:
	dune build @doc
	open _build/default/_doc/_html/diet/Diet/module-type-INTERVAL_SET/index.html || echo 'Try pointing your browser at _build/default/_doc/_html/index.html'

clean:
	dune clean
