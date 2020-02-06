# Plan for Q1 2020

*Note: this was the original, now outdated plan. Many ideas here are
still valid, but it's not what I'm currently building.*

## Data model

The document id has to be a string. It's either provided when storing
it or taken from the `_id` attribute. These have to match or an error
is raised.

The document has to be valid JSON. It can optionally contain a `_id`
attribute as per above.

A nested document will be lifted out and indexed separately. If it
doesn't have an `_id` attribute it will be stored under the content
hash of its normalised JSON string (sorted keys, single line, no extra
spaces). This allows component documents to be shared, and be without
their own history.

Every indexed document has a companion meta document. This document
contains the following keys:

* `_id` - `string`, the meta document id, of the form
  `meta_{_document_id}`.
* `_vt_time` - `string`, the valid time as a ISO date string with
  nanosecond precision.
* `_document` - `string`, the JSON string of the normalised document
  (see above).
* `_document_hash` - `string`, the hash of `_document`.
* `_document_id` - `string, the `_id` of the document it annotates.
* `_tx` - `string`, link to the transaction document.

Meta documents don't have their own meta documents.

The transaction document is shared between all documents written in a
transaction. It contains the following keys:

* `_id` - `string`, the transaction/document id, of the form
  `tx_{td_ix}`.
* `_tx_time` - `string`, the transaction time as a ISO date string
  with nanosecond precision.
* `_tx_id` - `number`.
* `_meta` - `string`, an optional component document containing meta
  data specified by the user when submitting the transaction.

Notes:
* The concept of entities is downplayed. Instead we simply have
  versions of documents.
* Queries can operate as expected over the meta and transaction
  documents.
* One can potentially stretch the model a bit so the entire
  bi-temporal engine can operate directly in Datalog, to be decided.

## Indexing

This data is stored and grouped based on the attribute name. The above
documents are indexed into a columnar format similar to this:

* `_document_id` - `string`.
* `_value` - one of `number`, `boolean`, `string` or `null`.
* `_vt_time_nanos` - `long`.
* `_tx_time_nanos` - `long`.

Potential extra columns:

* `_version` - `bytes`, the combination of `_vt_time_nanos` and
`_tt_time_nanos`. Likely to not be stored explicitly but be derived.
* `_deleted` - `boolean`, `_value` is always `null` if `true`. Might
  not be needed and Arrow's concept of `null` values could be used for
  the `_value` column instead.
* `_tx_id` - `long`, not likely necessary in the index.
* `_order` - `long`, potentially used for arrays.

The variable-length `strings` above will be stored using dictionary
encoding, either local per file, or potentially globally, where the
ids are tracked across several files.

JSON objects are lifted up as described above and their `_value` will
be their string `_id` in the index (which will be the `_document_hash`
for components). JSON arrays are stored as repeated
attributes. Ordering is specified inside the original `_document`.

The current `_version` of a document at a point in time can be found
via its `_id` attribute. Repeated attributes share the same version
and are all visible.

## Index Storage

The columnar data itself will be stored using memory mapped Arrow
files. Index chunks will have `min` and `max` and bloom-filter
metadata for their columns, and potentially also `_va` column, which
is a "vector approximation" of the other columns fitting inside say an
`_int` which can be quickly scanned for without touching the real
columns. Each file will be split into several record batches, each
with their own metadata so they can be skipped.

Alternatively, the file could also be stored as the leaves of the data
based on their k2-tree sort order, and include the k2-tree bitmap as
part of the metadata. This would allow range queries based on this
bitmap, at the cost of complexity.

These files themselves will be stored using a multidimensional
structure of the Grid File family (but preferably an immutable
variant), where each data bucket is a file.

## Querying

Queries will be done via simple Datalog with negation using
Prolog-syntax, like this:

```prolog
associated_with(Person, Movie) :- movie_cast(Movie, Person).
associated_with(Person, Movie) :- movie_director(Movie, Person).
query(Name) :- movie_title(Movie, "Predator"),
               associated_with(Person, Movie),
               person_name(Person, Name).
```

Documents themselves aren't used explicitly during query processing,
the data is served straight from the index. The original document can
be found via the `_document` attribute if needed.

The query engine itself will be implemented using lower-level
primitives directly supporting scans and bi-temporality on the column
format without being tied to the exact representation. Bi-temporality
itself will be implemented using lower-level primitives, similar to
the `aj` as of join in KDB. The query engine might optionally
construct better in-memory versions of the column data above, for
example by sorting it on different attributes.

The query engine should eventually support range queries in valid
time. This is a form of interval joins. The current suggested index
isn't great for finding the intervals as they're not explicitly
stored, so might need to be revisited.

## Ingestion

The documents might originate from somewhere like Kafka, but this is
left undecided as part of this work. There will be a way to update
several documents in a single transaction to the local files.

Deletion of a document sets all visible attributes to a `null`
tombstone. Adding a document which has fewer keys need to explicitly
delete the missing keys in the new version.

For range updates and interval queries to work, other modifications of
the index might also be needed.

## Distribution

The queries will always be run on one node, but the chunks built above
will be consistent once build (ignoring eviction) so they and their
meta data can separately be stored elsewhere and the indexing burden
potentially shared in various ways.

The local node would keep the meta-data of the existing chunks around
so it knows how what to fetch. A simple alternative is to have a
shared store for this so nodes can bootstrap themselves. This may be
Kafka, a RDBMS or simply S3.

## Eviction

Eviction is harder in this scheme, but essentially involves mutating
chunks and setting various fields to `null`. It's not explicitly part
of this work.

## Implementation Language

The goal is to have Clojure or Java versions implemented for all
layers, but optionally be able to swap out some for Rust, especially
in the lower layers.
