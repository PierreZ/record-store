# record-store ![record-store](https://github.com/PierreZ/record-store/workflows/record-store/badge.svg?branch=master) ![record-store](https://github.com/PierreZ/record-store/workflows/presto-connector/badge.svg?branch=master) ![record-store](https://github.com/PierreZ/record-store/workflows/java-client/badge.svg?branch=master)

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

## Access

`recordstore.pierrezemb.org:443`

* pierre: CioIABIGdGVuYW50EgZwaWVycmUaFgoUCAQSBAgAEAASBAgAEAcSBAgAEAgaIK9qDfu3+WHNAzDqfNIbe5ANvWVo5ieppvu+T1y/hwc2
* steven: CioIABIGdGVuYW50EgZzdGV2ZW4aFgoUCAQSBAgAEAASBAgAEAcSBAgAEAgaIAs7vb0irYUOrMig8hUhJ31j27X1C3RznDZl+Os29auU
