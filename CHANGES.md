## 0.9.1 (2017-02-25)
- Add configuration `runtime_assert` to check GC invariants at runtime
- Use tail-recursive calls in the block recycler (which deals with large
  block lists)
- Wait for the compaction work list to stabilise before processing it
  (otherwise we move blocks which are then immediately discarded)
- Track the difference between blocks on the end of the file being full
  of zeroes due to ftruncate versus being full of junk due to discard
- On open, truncate the file to erase trailing junk
- Don't try to use free space between header structures for user data
  since we assume all blocks after the start of free space are movable
  and header blocks aren't (in this implementation)
- Make cluster locks recursive, hold relevant metadata read locks while
  reading or writing data clusters to ensure they aren't moved while
  we're using them.
- Add a debug testing mode and use it in a test case to verify that
  compact mid-write works as expected.

## 0.9.0 (2017-02-21)
- Add online coalescing mode and background cluster recycling thread
- Rename internal modules and types
- Ensure the interval tree remains balanced to improve performance

## 0.8.1 (2017-02-13)
- fix error in META file

## 0.8.0 (2017-02-13)
- update to Mirage 3 APIs
- now requires OCaml 4.03+
- ensure the interval tree is kept balanced

## 0.7.2 (2016-12-21)
- if `discard` is not enabled, fail `discard` calls
- if `discard` is enabled, enable lazy-refcounts and zero refcount clusters
  to avoid breaking refcounts over `discard`, `compact`

## 0.7.1 (2016-12-15)
- speed up `check` and `compact` up to 50x
- `qcow-tool compact` work around files which aren't a whole number of
  sectors

## 0.7.0 (2016-12-10)
- now functorised over `TIME`
- allow background compact to be cancelled
- cancel background compact to allow regular I/O to go through
- don't trigger the background compact until 1s after the last
  `discard`
- on `connect`, sanity-check the image

## 0.6.0 (2016-12-04)
- rename ocamlfind package from `qcow-format` to `qcow` for uniformity
- add support for runtime configuration arguments to `connect` and `create`
- add support for `discard` (aka TRIM or UNMAP) and online compaction
  (through a stop-the-world GC)
- switch the build from `oasis` to `topkg` (thanks to @jgimenez)

## 0.5.0 (2016-11-26)
- `resize` now takes a new size in bytes (rather than sectors) and uses a
  labelled argument
- `qcow-tool info` now takes a `--filter <expression>` for example
  `qcow-tool info ... --filter .size` to view the virtual size

## 0.4.2 (2016-09-21)
- Don't break the build if `Block.connect` has optional arguments

## 0.4.1 (2016-08-17)
- Remove one necessary source of `flush` calls
- CLI: add `mapped` command to list the mapped regions of a file

## 0.4 (2016-08-03)
- For buffered block devices, call `flush` to guarantee metadata correctness
- In lazy_refcounts mode (the default), do not compute any refcounts
- CLI: the `repair` command should recompute refcounts

## 0.3 (2016-05-12)
- Depend on ppx, require OCaml 4.02+

## 0.2 (2016-01-15)
- Use qcow version 3 by default, setting `lazy_refcount=on`
- Unit tests now verify that `qemu-img check` is happy and that `qemu-nbd`
  sees the same data we wrote

## 0.1 (2015-11-09)
- initial `V1_LWT.BLOCK` support
- caches metadata for performance
- CLI tool for manipulating images
- supports the `seek_mapped` `seek_unmapped` interface for iterating over
  sparse regions
