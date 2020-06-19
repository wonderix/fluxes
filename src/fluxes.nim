import asyncdispatch, options, asyncfile

type
  Flux*[T] = ref object
    next: proc(): Future[Option[T]]
    cancel*: proc()

proc newFlux*[T](it: iterator(): T): Flux[T] =
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

proc map*[T,S](f: Flux[T], m: proc(t: T): S): Flux[S] =
  new(result)
  result.next = proc() : Future[Option[S]] = 
    let f = newFuture[Option[S]]()
    let x = f.next()
    x.callback = proc() =
      if x.failed:
        f.fail(x.readError())
      else:
        try:
          f.complete(x.read().map(m))
        except:
          f.fail(getCurrentException())
    return f
  result.cancel = f.cancel

iterator items*[T](f: Flux[T]):T {.closure.} =
    while true:
      let x = waitFor f.next()
      if x.isSome():
        yield x.get()
      else:
        break


