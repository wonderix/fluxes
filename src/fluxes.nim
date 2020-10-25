import asyncdispatch
import options
import asyncfile
import asyncnet
import sets
from sequtils import repeat
from sugar import `=>`

## This module implements asynchronous streams.
##
## This module is heavily inspired by `project reactor <https://projectreactor.io/>`_ and the ``sequtils`` package

type
  Flux*[T] = ref object
    next: proc(): Future[Option[T]] {.gcsafe.}
    cancel*: proc() {.gcsafe.} 

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

proc newFlux*[T](it: iterator(): T {.gcsafe.}): Flux[T] =
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
  result.next = proc() : Future[Option[T]]= 
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
    import asyncdispatch
    from sugar import `=>`
    writeFile("/tmp/flux.txt","Hello World\n")
    let flux = newFlux(openAsync("/tmp/flux.txt"),bufferSize = 8)
    let content = waitFor flux.foldr((s: string,t: string) => s & t,"")
    assert content == "Hello World\n"

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
    from sugar import `=>`
    proc serve() {.async.} =
      var server = newAsyncSocket()
      server.setSockOpt(OptReuseAddr, true)
      server.bindAddr(Port(14365))
      server.listen()
      let c = await server.accept()
      await c.send("Hello World\n")
      c.close()
      server.close()

    asyncCheck serve()
    let flux = newFlux(waitFor asyncnet.dial("localhost",Port(14365)))
    let content = waitFor flux.foldr((s: string,t: string) => s & t,"")
    assert content == "Hello World\n"

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

proc repeat*[T](x: T, n: Natural): Flux[T] =
  ## Returns a new flux with the item `x` repeated `n` times.
  ## `n` must be a non-negative number (zero or more).
  ##
  runnableExamples:
    let
      total = repeat(5, 3)
    assert total.toSeq() == @[5, 5, 5]
  
  result = newFlux(sequtils.repeat(x,n))

proc deduplicate*[T](f: Flux[T], isSorted: bool = false): Flux[T] =
  ## Returns a new sequence without duplicates.
  ##
  ## Setting the optional argument ``isSorted`` to ``true`` (default: false)
  ## uses a faster algorithm for deduplication.
  ##
  runnableExamples:
    let
      dup1 = newFlux(@[1, 1, 3, 4, 2, 2, 8, 1, 4])
      dup2 = newFlux(@["a", "a", "c", "d", "d"])
      unique1 = deduplicate(dup1)
      unique2 = deduplicate(dup2, isSorted = true)
    assert unique1.toSeq() == @[1, 3, 4, 2, 8]
    assert unique2.toSeq() == @["a", "c", "d"]

  new(result)
  if isSorted:
    var prev :ref Option[T] = new Option[T]
    proc readNext[T](f: Flux[T], future: Future[Option[T]]) =
        let next = f.next()
        next.addCallback(proc() =
          if next.failed:
            future.fail(next.readError())
          else:
            try:
              let o = next.read()
              if o == prev[]:
                readNext(f,future)
              else:
                future.complete(o)
                prev[] = o
            except:
              future.fail(getCurrentException())
        )
    result.next = proc() : Future[Option[T]] = 
      result = newFuture[Option[T]]()
      readNext(f,result)
  else:
    var seen :HashSet[T]
    init(seen)
    proc readNext[T](f: Flux[T], future: Future[Option[T]]) =
        let next = f.next()
        next.addCallback(proc() =
          if next.failed:
            future.fail(next.readError())
          else:
            try:
              let o = next.read()
              if o.isSome():
                let t = o.get()
                if seen.contains(t):
                  readNext(f,future)
                else:
                  future.complete(o)
                  seen.incl(t)
              else:
                future.complete(o)
            except:
              future.fail(getCurrentException())
        )
    result.next = proc() : Future[Option[T]] = 
      result = newFuture[Option[T]]()
      readNext(f,result)
  result.cancel = f.cancel


proc map*[T,S](f: Flux[T], op: proc(t: T): S {.gcsafe.}): Flux[S] =
  ## Returns a new `Flux` with the results of `op` proc applied to every
  ## item in the `Flux` `f`.
  ##
  runnableExamples:
    import asyncdispatch
    from sugar import `=>`
    let
      f = newFlux(@[1, 2, 3, 4])
      b = f.map((x: int) => $x).toSeq()
    assert b == @["1", "2", "3", "4"]
  
  new(result)
  result.next = proc() : Future[Option[S]] = 
    let future = newFuture[Option[S]]()
    let next: Future[Option[T]] = f.next()
    next.addCallback(proc() =
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



proc concat*[T](fluxes: varargs[Flux[T]]): Flux[T] =
  ## Takes several fluxes' items and returns them inside a new fluxes. All fluxes must be of the same type.
  runnableExamples:
    import asyncdispatch
    let
      f1 = newFlux(@[1, 2, 3])
      f2 = newFlux(@[4, 5])
      f3 = newFlux(@[6, 7])
      total = concat(f1, f2, f3)
    assert total.toSeq() == @[1, 2, 3, 4, 5, 6, 7]
  new(result)
  var index :ref int = new int
  var copiedFluxes = newSeqOfCap[Flux[T]](fluxes.len)
  for f in fluxes:
    copiedFluxes.add(f)
  proc readNext[T](future: Future[Option[T]]) =
    if index[] >= copiedFluxes.len:
      future.complete(none(T))
    else:
      let next: Future[Option[T]] = copiedFluxes[index[]].next()
      next.addCallback(proc()=
        if next.failed:
          future.fail(next.readError())
        else:
          let r = next.read()
          if r.isSome():
            future.complete(r)
          else:
            index[].inc
            readNext(future)
      )
  result.next = proc() : Future[Option[T]] = 
    result = newFuture[Option[T]]()
    readNext(result)

  result.cancel = proc() =
     while index[] < len(copiedFluxes):
       copiedFluxes[index[]].cancel()
       index[].inc

proc foldr*[T,A](f: Flux[T], accumulator: proc(a: A, t: T): A, initial: A): Future[A] {.async.}=
  ## Fold a flux from left to right, returning the accumulation.
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



proc filter*[T](f: Flux[T]; pred: proc (x: T): bool  {.gcsafe.}): Flux[T] =
  ## Returns a new flux with all the items of `f` that fulfilled the
  ## predicate `pred` (function that returns a `bool`).
  ##
  runnableExamples:
    from sugar import `=>`
    let
      colors = newFlux(@["red", "yellow", "black"])
      f = filter(colors, (x: string) => x.len < 6).toSeq
    assert f == @["red", "black"]

  new(result)
  let predicate = pred
  proc readNext[T](f: Flux[T], future: Future[Option[T]]) =
    let next = f.next()
    next.addCallback(proc() =
      if next.failed:
        future.fail(next.readError())
      else:
        try:
          let o = next.read()
          if o.isSome():
            let y: T = o.get
            if predicate(y):
              future.complete(o)
            else:
              readNext(f,future)
          else:
            future.complete(o)
        except:
          future.fail(getCurrentException())
    )
  result.next = proc() : Future[Option[T]] = 
    let future = newFuture[Option[T]]()
    readNext(f,future)
    return future
  result.cancel = f.cancel


proc keep*[T](f: Flux[T], pred: proc(x: T): bool {.gcsafe.}): Flux[T] =
  ## Keeps the items in the passed flux `f` if they fulfilled the
  ## predicate `pred` (function that returns a `bool`).
  ##
  ##
  runnableExamples:
    var floats = newFlux(@[13.0, 12.5, 5.8, 2.0, 6.1, 9.9, 10.1])
    floats = floats.keep(proc(x: float): bool = x > 10)
    assert floats.toSeq() == @[13.0, 12.5, 10.1]
  
  return filter(f,proc(x: T): bool = pred(x))

proc zip*[S,T](f1: Flux[S],f2: Flux[T]): Flux[(S,T)]=
  ## Returns a new flux with a combination of the two input fluxes.
  ##
  ## The input fluxes can be of different types.
  ## If one flux is shorter, the remaining items in the longer flux
  ## are discarded.
  ##
  runnableExamples:
    import asyncdispatch
    let
      short1 = newFlux(@[1, 2, 3])
      long = newFlux(@[6, 5, 4, 3, 2, 1])
      zip1 = zip(short1, long).toSeq()
    assert zip1 == @[(1, 6), (2, 5), (3, 4)]
 
    let  
      short2 = newFlux(@[1, 2, 3])
      words = newFlux(@["one", "two", "three"])
      zip2 = zip(short2, words).toSeq()
    assert zip2 == @[(1, "one"), (2, "two"), (3, "three")]

  new(result)
  result.next = proc() : Future[Option[(S,T)]] = 
    let future = newFuture[Option[(S,T)]]()
    let n1: Future[Option[S]] = f1.next()
    let n2: Future[Option[T]] = f2.next()
    (n1 and n2).addCallback(proc() =
      if n1.failed:
        future.fail(n1.readError())
      elif n2.failed:
        future.fail(n2.readError())
      else:
        try:
          let o1 = n1.read()
          let o2 = n2.read()
          if o1.isSome() and o2.isSome():
            future.complete(some((o1.get(),o2.get())))
          else:
            if o1.isNone() and o2.isNone():
              discard
            elif o1.isNone():
              f2.cancel()
            else:
              f1.cancel()
            future.complete(none((S,T)))
        except:
          future.fail(getCurrentException())
    )
    return future
  result.cancel = () => f1.cancel(); f2.cancel()

proc all*[T](f: Flux[T], pred: proc(x: T): bool {.gcsafe.}): Future[bool] =
  ## Iterates through a flux and checks if every item fulfills the
  ## predicate.
  ##
  ##
  runnableExamples:
    import asyncdispatch
    from sugar import `=>`

    let numbers = @[1, 4, 5, 8, 9, 7, 4]
    assert (waitFor newFlux(numbers).all((x: int) => x < 10)) == true
    assert (waitFor newFlux(numbers).all((x: int) => x < 9)) == false

  let future = newFuture[bool]()
  let predicate = pred
  proc readNext[T](f: Flux[T]) =
    let next = f.next()
    next.addCallback(proc() =
      if next.failed:
        future.fail(next.readError())
      else:
        try:
          let o = next.read()
          if o.isSome():
            let y: T = o.get
            if not predicate(y):
              future.complete(false)
              f.cancel()
            else:
              readNext(f)
          else:
            future.complete(true)
        except:
          future.fail(getCurrentException())
    )
  readNext(f)
  return future

proc toSeqFutureImpl[T](f: Flux[T]): Future[seq[T]] {.async.} =
  ## Convert a `Flux` to a `seq`
  runnableExamples:
    import asyncdispatch
    let
      f = newFlux(@[3, 2, 1])
    assert f.toSeq() == @[3, 2, 1]
  var a = newSeq[T]()
  while true:
    let next = await f.next()
    if next.isSome():
      a.add(next.get())
    else:
      return a

converter toSeqFuture*[T](f: Flux[T]): Future[seq[T]] =
  ## Convert a flux to a sequence Future
  f.toSeqFutureImpl()

converter toSeq*[T](f: Flux[T]): seq[T] =
  ## Convert a flux to a sequence
  waitFor f.toSeqFutureImpl()

iterator items*[T](f: Flux[T]):T {.closure.} =
    while true:
      let x = waitFor f.next()
      if x.isSome():
        yield x.get()
      else:
        break


proc readAll*(f: Flux[string]): owned(Future[string]) {.async.} =
  ## Returns a future that will complete when all the string data from the
  ## specified flux is retrieved.
  result = ""
  while true:
    let value = await f.next()
    if value.isSome():
      result.add(value.get())
    else:
      break
