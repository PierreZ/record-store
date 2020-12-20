# record-store [![status](https://img.shields.io/badge/status-experimental-red)](https://github.com/PierreZ/record-store) ![record-store](https://github.com/PierreZ/record-store/workflows/record-store/badge.svg?branch=master) ![java-client](https://github.com/PierreZ/record-store/workflows/java-client/badge.svg?branch=master) ![testcontainers-foundationdb](https://github.com/PierreZ/record-store/workflows/testcontainers-foundationdb/badge.svg?branch=master)

![Logo](./docs/art/logo.png)

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

## Test it

```bash
# deploy your fdb cluster, or use docker
docker run -d --name fdb -p 4500:4500 foundationdb/foundationdb:6.2.19

# init fdb
docker exec fdb fdbcli --exec "configure new single memory"

# wait until it is ready
docker exec fdb fdbcli --exec "status"

# generate cluster file
echo "docker:docker@127.0.0.1:4500" > fdb.cluster

# retrieve latest version
wget https://github.com/PierreZ/record-store/releases/download/v0.0.1/record-store-v0.0.1-SNAPSHOT-fat.jar

# retrieve config file example, don't forget to edit it if necessary
wget https://raw.githubusercontent.com/PierreZ/record-store/master/config.json

# run fat jar
java -jar record-store-v0.0.1-SNAPSHOT-fat.jar -conf ./config.json
```

## Building the Record-Layer

### Requirements

* JDK 11 or more
* Docker (for testing)
* gradle 6.2.2
* [FoundationDB Client Packages](https://www.foundationdb.org/download/)

### Configure IntelliJ

Please enable annotation processing and obtain them from project classpath.

### Gradle cheat-sheet

To launch your tests:
```
./gradlew :record-store:test
```

To package your application:
```
./gradlew :record-store:assemble
```

To run your application:
```
./gradlew :record-store:run
```

To format:
```bash
gradle :java:spotlessApply
```
