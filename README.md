# Asynchronous streams for nim

This project provides asynchronous streams for nim.

This project is inspired by [project reactor](https://projectreactor.io).

It is the asynchronous counterpart of sequtils.


## Example

```python
import fluxes
import asyncdispatch
from sugar import `=>`
let
    f = newFlux(@[1, 2, 3, 4])
    b = f.map((x: int) => $x).toSeq()
assert b == @["1", "2", "3", "4"]
```


## Documentation

The documentation can be found [here](https://htmlpreview.github.io/?https://github.com/wonderix/fluxes/blob/master/src/htmldocs/fluxes.html)

