# crux.rs

**Experimental**

Parts of Crux re-imagined in Rust (nightly).

See https://github.com/juxt/crux

[Plan for Q1 2020](plan.md)

## Scope

The plan of attack using Rust+Arrow to build a bitemporal Datalog
engine based on columnar formats using a set of primitives somewhat
inspired by KDB to implement the engine itself.

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

## Building

First, you need `rustup`, see https://www.rust-lang.org/tools/install

You also need `libclang` installed, as the wrappers for the C
libraries depend on this to build. On Ubuntu this can be done via `apt
install llvm-dev libclang-dev clang`.

```bash
cargo build
```

## Linting

You need Clippy installed via `rustup component add clippy`. To lint
everything, including examples:

```bash
cargo clippy --all-targets
```

## Running the Kafka KV Example

You need Kafka and Zookeeper running, see https://kafka.apache.org/quickstart

``` bash
RUST_LOG=debug BOOTSTRAP_SERVERS=localhost:9092 cargo run --example kafka_kv_store
```

KV data is stored under `data`.

## Copyright & License

The MIT License (MIT)

Copyright © 2019 JUXT LTD.

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
