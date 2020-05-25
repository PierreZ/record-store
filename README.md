# record-store ![record-store](https://github.com/PierreZ/record-store/workflows/record-store/badge.svg?branch=master) ![java-client](https://github.com/PierreZ/record-store/workflows/java-client/badge.svg?branch=master)

## Building

### Requirements

* JDK 11 or more
* Docker (for testing)
* gradle 6.2.2
* [FoundationDB Client Packages](https://www.foundationdb.org/download/)

### Configure IntelliJ

Please enable annotation processing and obtain them frm project classpath.

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

