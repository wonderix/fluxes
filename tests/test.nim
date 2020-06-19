# This is just an example to get you started. You may wish to put all of your
# tests into a single file, or separate them into multiple `test1`, `test2`
# etc. files (better names are recommended, just make sure the name starts with
# the letter 't').
#
# To run these tests, simply execute `nimble test`.

import unittest
import fluxes
import asyncfile

test "Iterator based flux":
  iterator myiterator(): int {.closure.}  =
    for x in 0..2:
      yield x

  let flux = newFlux(myiterator)
  var x = 0
  for y in flux.items():
    doAssert y == x
    x += 1


test "File based flux":
  writeFile("/tmp/flux.txt","Hello World\n")
  let asyncFile = openAsync("/tmp/flux.txt")
  let flux = newFlux(asyncFile,bufferSize = 8)
  var x = 0
  for y in flux.items():
    case x:
    of 0:
      doAssert y == "Hello Wo"
    of 1:
      doAssert y == "rld\n"
    else:
      doAssert false
    x += 1

