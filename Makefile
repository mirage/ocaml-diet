
.PHONY: build clean test

build:
	jbuilder build @install

test:
	jbuilder build lib_test/compact_random.exe lib_test/test.exe
	./_build/default/lib_test/compact_random.exe -compact-mid-write -stop-after 16
	./_build/default/lib_test/test.exe -runner sequential

install:
	jbuilder install

uninstall:
	jbuilder uninstall

clean:
	rm -rf _build
