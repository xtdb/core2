# Crux Datalog

*Multidimensional Datalog Engine*

**Experimental**

See https://github.com/juxt/crux

## Scope

*Update, Feb 2020:* I'm now building a Datalog engine with
Prolog-syntax. It's written in Clojure and based on Apache
Arrow. Joins will be supported using a single, multi-dimensional,
index. Storage will be distributed.

While its scope is smaller, the ambition is that it will do the things
it does with speed and economy.

It's lower-level than Crux, but could become useful on its own. Parts
might later be ported to Rust. Crux bitemporal semantics will be
possible to implement on top of it.

*Earlier brief, still somewhat true:* The plan of attack using
Rust+Arrow to build a bitemporal Datalog engine based on columnar
formats using a set of primitives somewhat inspired by KDB to
implement the engine itself. See spikes under [rust](rust).

### Goals

* Apache Arrow
* JSON documents
* Worst Case Optimal Joins
* Basic Graph Patterns only
* Bitemp implemented as lower level join operators
* Point-in-time and valid-time range queries
* Range update semantics
* Nanosecond precision
* Clojure to Rust JNR bridge

### Stretch Goals

* LUBM / WatDiv
* Layered architecture for comparing performance and complexity of modules
* Succinct indexes
* Ingest
* Remote storage

## Copyright & License

The MIT License (MIT)

Copyright © 2019-2020 JUXT LTD.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
