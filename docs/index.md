---
---

# record-store

[![github](https://img.shields.io/github/stars/PierreZ/record-store.svg?style=social)](https://github.com/PierreZ/record-store)

A light, multi-model, user-defined place for your data.

## Features

* **Light** We created the notion of `RecordSpace`, something that is lighter than a full table. Start a `RecordSpace` for any kind of data than you need to manage. With it, you can imagine start some `RecordSpaces` for each integrations tests. Or per environment. The choice is yours.

* **multi-tenant** A `tenant` can create as many `RecordSpace` as we wants

* **Developer-oriented experience** We care about developers, so we created the perfect place for them to store data thanks to schemas and indexes.

* **Scalable** We are based on the same tech behind [CloudKit](https://www.foundationdb.org/files/record-layer-paper.pdf) called the [Record Layer](https://github.com/foundationdb/fdb-record-layer/). CloudKit uses the Record Layer to host billions of independent databases. The name of this project itself is a tribute to the Record Layer. We are exposing the layer within a gRPC protocol.

* **Encrypted** Data are encrypted by default.

* **Multi-model** For each `RecordSpace`, you can define a `schema`, which is in-fact only a `Protobuf` definition. You need to store some `users`, or a more complicated structure? If you can represent it as `Protobuf`, you are good to go!

* **Index-defined queries** Your queries's capabilities are defined by the indexes you put on your schema. No more full-scan!

