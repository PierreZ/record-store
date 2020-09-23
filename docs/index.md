---
---

# Record-Store

[![status](https://img.shields.io/badge/status-experimental-red)](https://github.com/PierreZ/record-store)
[![github](https://img.shields.io/github/stars/PierreZ/record-store.svg?style=social)](https://github.com/PierreZ/record-store)

A light, multi-model, user-defined place for your data.

## Features

* **Light** We created the notion of `RecordSpace`, which can be viewed as a Collection. Start a `RecordSpace` for any kind of data than you need to manage. With it, you can imagine start some `RecordSpaces` for each integrations tests. Or per environment. The choice is yours.

* **Multi-tenant** A `tenant` can create as many `RecordSpace` as we want, and we can have many `tenants`.

* **Standard API** We are exposing the record-store with standard technologies:
    * [gRPC](https://grpc.io)
    * *very experimental* [GraphQL](https://graphql.org)

* **Scalable** We are based on the same tech behind [CloudKit](https://www.foundationdb.org/files/record-layer-paper.pdf) called the [Record Layer](https://github.com/foundationdb/fdb-record-layer/). CloudKit uses the Record Layer to host billions of independent databases. The name of this project itself is a tribute to the Record Layer as we are exposing the layer within a gRPC interface.

* **Transactional** We are running on top of [FoundationDB](https://www.foundationdb.org/). FoundationDB gives you the power of ACID transactions in a distributed database.

* **Encrypted** Data are encrypted by default.

* **Multi-model** For each `RecordSpace`, you can define a `schema`, which is in-fact only a `Protobuf` definition. You need to store some `users`, or a more complicated structure? If you can represent it as [Protobuf](https://developers.google.com/protocol-buffers), you are good to go!

* **Index-defined queries** Your queries's capabilities are defined by the indexes you put on your schema.

* **Secured** We are using [Biscuit](https://github.com/CleverCloud/biscuit), a mix of `JWT` and `Macaroons` to ensure auth{entication, orization}.

