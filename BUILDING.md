# Building the Record-Layer

## Requirements

* JDK 11 or more
* Docker (for testing)
* gradle 6.2.2
* [FoundationDB Client Packages](https://www.foundationdb.org/download/)

## Configure IntelliJ

Please enable annotation processing and obtain them from project classpath.

## Gradle cheat-sheet

### Record-store

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

