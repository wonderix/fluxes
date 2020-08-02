import asyncdispatch
import options
import asyncfile
import asyncnet
from sugar import `=>`

## This module implements asynchronous streams.
##
## This module is heavily inspired by `project reactor <https://projectreactor.io/>`_ and the ``sequtils`` package

type
  Flux*[T] = ref object
    next: proc(): Future[Option[T]]
    cancel*: proc()

proc newFlux*[T](s: openArray[T]): Flux[T] =
  ## Create a new ``Flux`` based on an sequence
  runnableExamples:
    let flux = newFlux(@[1, 2 ,3])
    var x = 1
    for y in flux.items():
      assert y == x
      x += 1

  var a = newSeq[T](s.len)
  for i, t in s:
    a[i] = t
  new(result)
  var i = 0
  result.next = proc() : Future[Option[T]] = 
    let f = newFuture[Option[T]]()
    if i >= a.len:
      f.complete(none[T]())
    else:  
      f.complete(some(a[i]))
      i += 1
    return f
  result.cancel = proc() = discard

proc newFlux*[T](it: iterator(): T): Flux[T] =
  ## Create a new ``Flux`` based on an iterator
  runnableExamples:
    iterator myiterator(): int {.closure.}  =
      for x in 0..5:
        yield x

    let flux = newFlux(myiterator)
    var x = 0
    for y in flux.items():
      assert y == x
      x += 1

  new(result)
  result.next = proc() : Future[Option[T]] = 
    let f = newFuture[Option[T]]()
    let x = it()
    if finished(it):
      f.complete(none[T]())
    else:  
      f.complete(some(x))
    return f
  result.cancel = proc() = discard

proc newFlux*(af: AsyncFile, bufferSize: int): Flux[string] =
  ## Create a new ``Flux`` based on an ``AsyncFile``
  runnableExamples:
    import asyncfile
    writeFile("/tmp/flux.txt","Hello World\n")
    let flux = newFlux(openAsync("/tmp/flux.txt"),bufferSize = 8)
    var buffer = ""
    for y in flux.items():
      buffer = buffer & y
    assert buffer == "Hello World\n"

  new(result)
  result.next = proc() : Future[Option[string]] = 
    let f = newFuture[Option[string]]()
    let x = af.read(bufferSize)
    x.callback = proc() =
      if x.failed:
        f.fail(x.readError())
      else:
        let s = x.read()
        if s.len() == 0:
          f.complete(none(string))
          af.close()
        else:
          f.complete(some(s))
    return f
  result.cancel = proc() = af.close()

proc newFlux*(ass: AsyncSocket, bufferSize = 1024): Flux[string] =
  ## Create a new ``Flux`` based on an ``AsyncSocket``
  runnableExamples:
    import asyncdispatch
    import asyncnet
    proc serve() {.async.} =
      var server = newAsyncSocket()
      server.setSockOpt(OptReuseAddr, true)
      server.bindAddr(Port(14365))
      server.listen()
      let c = await server.accept()
      await c.send("Hello World\n")
      c.close()
      server.close()

    proc client(): Future[Flux[string]]  {.async.} =
      return newFlux(await asyncnet.dial("localhost",Port(14365)))

    asyncCheck serve()
    let flux = waitFor(client())
    var buffer = ""
    for token in flux.items():
      buffer = buffer & token
    assert buffer == "Hello World\n"

  new(result)
  result.next = proc() : Future[Option[string]] = 
    let f = newFuture[Option[string]]()
    let x = ass.recv(bufferSize)
    x.callback = proc() =
      if x.failed:
        f.fail(x.readError())
      else:
        let s = x.read()
        if s.len() == 0:
          f.complete(none(string))
          ass.close()
        else:
          f.complete(some(s))
    return f
  result.cancel = proc() = ass.close()


proc map*[T,S](f: Flux[T], op: proc(t: T): S {.gcsafe.}): Flux[S] =
  ## Returns a new `Flux` with the results of `op` proc applied to every
  ## item in the `Flux` `f`.
  ##
  runnableExamples:
    import asyncdispatch
    let
      a = @[1, 2, 3, 4]
      b = waitFor(newFlux(a).map(proc(x: int): string = $x).toSeqFuture())
    assert b == @["1", "2", "3", "4"]
  
  new(result)
  result.next = proc() : Future[Option[S]] = 
    let future = newFuture[Option[S]]()
    let next: Future[Option[T]] = f.next()
    next.addCallback(proc() {.gcsafe.} =
      if next.failed:
        future.fail(next.readError())
      else:
        try:
          future.complete(next.read().map(op))
        except:
          future.fail(getCurrentException())
    )
    return future
  result.cancel = f.cancel


proc concatReadNext[T]( index: ref int, fluxes: seq[Flux[T]], future: Future[Option[T]]) =
  if index[] >= fluxes.len:
    future.complete(none(T))
  else:
    let next: Future[Option[T]] = fluxes[index[]].next()
    next.addCallback(proc() {.gcsafe.} =
      if next.failed:
        future.fail(next.readError())
      else:
        let r = next.read()
        if r.isSome():
          future.complete(r)
        else:
          index[].inc
          concatReadNext(index,fluxes,future)
    )

proc concat*[T](fluxes: varargs[Flux[T]]): Flux[T] =
  ## Takes several fluxes' items and returns them inside a new fluxes. All fluxes must be of the same type.
  runnableExamples:
    import asyncdispatch
    let
      f1 = newFlux(@[1, 2, 3])
      f2 = newFlux(@[4, 5])
      f3 = newFlux(@[6, 7])
      total = concat(f1, f2, f3)
    assert (waitFor total.toSeqFuture()) == @[1, 2, 3, 4, 5, 6, 7]
  new(result)
  var index :ref int = new int
  var fl = newSeqOfCap[Flux[T]](fluxes.len)
  for f in fluxes:
    fl.add(f)
  result.next = proc() : Future[Option[T]] = 
    let future = newFuture[Option[T]]()
    concatReadNext(index,fl,future)
    return future
  result.cancel = proc() =
     while index[] < len(fl):
       fl[index[]].cancel()
       index[].inc

proc foldr*[T,A](f: Flux[T], accumulator: proc(a: A, t: T): A, initial: A): Future[A] {.async.}=
  ## Reduce the values from this Flux into a single object matching the type of a seed value.
  runnableExamples:
    import asyncdispatch
    from sugar import `=>`
    let
      f = newFlux(@[1, 2, 3])
    assert (waitFor f.foldr((a: int,x: int) => a+x, 0)) == 6
  var acc: A
  acc = initial
  while true:
    let next = await f.next()
    if next.isSome():
      acc = accumulator(acc,next.get())
    else:
      return acc

proc count*[T](f: Flux[T], x : T): Future[int] =
  ## Returns the number of occurrences of the item x in the flux f.
  runnableExamples:
    import asyncdispatch
    let
      f = newFlux(@[1, 2, 2, 3, 2, 4, 2])
    assert (waitFor f.count(2)) == 4
  return foldr(f,(a: int,t: T) => (if t == x: a + 1 else: a), 0)

proc toSeqFuture*[T](f: Flux[T]): Future[seq[T]] {.async.} =
  ## Convert a `Flux` to a `seq`
  runnableExamples:
    import asyncdispatch
    let
      f = newFlux(@[3, 2, 1])
    assert (waitFor toSeqFuture(f)) == @[3, 2, 1]
  var a = newSeq[T]()
  while true:
    let next = await f.next()
    if next.isSome():
      a.add(next.get())
    else:
      return a

iterator items*[T](f: Flux[T]):T {.closure.} =
    while true:
      let x = waitFor f.next()
      if x.isSome():
        yield x.get()
      else:
        break
