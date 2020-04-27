# record-store

## Building

### Requirements

* JDK 11 or more
* Docker (for testing)
* gradle 6.2.2
* [FoundationDB Client Packages](https://www.foundationdb.org/download/)


### Gradle cheat-sheet

#### Record-store

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

#### Presto-connector
