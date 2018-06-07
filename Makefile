
.PHONY: build clean test fuzz

build:
	jbuilder build @install

test:
	jbuilder build lib_test/test.exe
	./_build/default/lib_test/test.exe -runner sequential

fuzz:
	jbuilder build fuzz/fuzz.exe
	mkdir -p test/input
	echo abcd > test/input/case
	afl-fuzz -i test/input -o output ./_build/default/fuzz/fuzz.exe @@

doc:
	jbuilder build @doc
	open _build/default/_doc/_html/diet/Diet/module-type-INTERVAL_SET/index.html || echo 'Try pointing your browser at _build/default/_doc/_html/index.html'

doc-update:
	jbuilder build @doc
	cp -R _build/default/_doc/_html ./html
	git checkout gh-pages
	cp -R html/* .
	rm -rf html
	git commit -a -s -m 'Update gh-pages'
	git push origin gh-pages
	git checkout master

install:
	jbuilder install

uninstall:
	jbuilder uninstall

clean:
	rm -rf _build
