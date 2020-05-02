# record-store

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
