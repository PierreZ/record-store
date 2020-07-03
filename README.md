# record-store ![record-store](https://github.com/PierreZ/record-store/workflows/record-store/badge.svg?branch=master) ![java-client](https://github.com/PierreZ/record-store/workflows/java-client/badge.svg?branch=master) ![testcontainers-foundationdb](https://github.com/PierreZ/record-store/workflows/testcontainers-foundationdb/badge.svg?branch=master)

A light, multi-model, user-defined place for your data.

## Features

* **Light** We created the notion of `RecordSpace`, something that is lighter than a full table. Start a `RecordSpace` for any kind of data than you need to manage. With it, you can imagine start some `RecordSpaces` for each integrations tests. Or per environment. The choice is yours.

* **Developer-oriented experience** We care about developers, so we created the perfect place for them to store data.

* **Scalable** We are based on the same tech behind [CloudKit](https://www.foundationdb.org/files/record-layer-paper.pdf) called the Record Layer. `CloudKit uses the Record Layer to host billions of independent databases`. The name of this project itself is a tribute to the Record Layer. We are exposing the layer within a gRPC protocol.

* **Encrypted** Data are encrypted by default.

* **Multi-model** For each `RecordSpace`, you can define a `schema`, which is in-fact only a `Protobuf` definition. You need to store some `users`, or a more complicated structure? If you can represent it as `Protobuf`, you are good to go!

* **Index-defined queries** Your queries's capacities is defined by the indexes you put on your schema

## Building and contribute

See the following guide [./BUILDING.md](./BUILDING.md)
