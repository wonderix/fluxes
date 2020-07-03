import asyncdispatch, options, asyncfile

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
    for x in flux.items():
      echo x

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
      for x in 0..2:
        yield x
    let flux = newFlux(myiterator)
    for x in flux.items():
      echo x

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
        else:
          f.complete(some(s))
    return f
  result.cancel = proc() = af.close()

proc newFlux*(af: AsyncFD, bufferSize = 1024): Flux[string] =
  ## Create a new ``Flux`` based on an ``AsyncFD``
  new(result)
  result.next = proc() : Future[Option[string]] = 
    let f = newFuture[Option[string]]()
    let x = af.recv(bufferSize)
    x.callback = proc() =
      if x.failed:
        f.fail(x.readError())
      else:
        let s = x.read()
        if s.len() == 0:
          f.complete(none(string))
        else:
          f.complete(some(s))
    return f
  result.cancel = proc() = af.closeSocket()

converter toSeq*[T](f: Flux[T]): seq[T] =
  ## Convert a `Flux` to a `seq`
  result = newSeq[T]()
  for i in f.items:
    result.add(i)

proc map*[T,S](f: Flux[T], op: proc(t: T): S {.gcsafe.}): Flux[S] =
  ## Returns a new `Flux` with the results of `op` proc applied to every
  ## item in the `Flux` `f`.
  ##
  runnableExamples:
    let
      a = @[1, 2, 3, 4]
      b = newFlux(a).map(proc(x: int): string = $x).toSeq()
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

iterator items*[T](f: Flux[T]):T {.closure.} =
    while true:
      let x = waitFor f.next()
      if x.isSome():
        yield x.get()
      else:
        break


