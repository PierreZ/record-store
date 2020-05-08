# record-store ![gradle build](https://github.com/PierreZ/record-store/workflows/gradle%20build/badge.svg?branch=master)

## Building

### Requirements

* JDK 11 or more
* Docker (for testing)
* gradle 6.2.2
* [FoundationDB Client Packages](https://www.foundationdb.org/download/)


### Gradle cheat-sheet

To launch your tests:
```bash
./gradlew clean test
```

To package your application:
```bash
./gradlew clean assemble
```

To run your application:
```bash
docker run -it --rm --name fdb -p 4500:4500 foundationdb/foundationdb:6.2.19
docker exec fdb fdbcli --exec "configure new single memory"
docker exec fdb fdbcli --exec "status"
# wait for it to be healthy

./gradlew clean run
```

## Access

`https://recordstore.pierrezemb.org`

* pierre: CioIABIGdGVuYW50EgZwaWVycmUaFgoUCAQSBAgAEAASBAgAEAcSBAgAEAgaIK9qDfu3+WHNAzDqfNIbe5ANvWVo5ieppvu+T1y/hwc2
* steven: CioIABIGdGVuYW50EgZzdGV2ZW4aFgoUCAQSBAgAEAASBAgAEAcSBAgAEAgaIAs7vb0irYUOrMig8hUhJ31j27X1C3RznDZl+Os29auU
