---
title: Managed schemas
---

In addition to the `recordSpaces` which allow you to define your own schemas, you also have the possibility to use `managedSchemas`. They are offering their own APIs and gRPC endpoints with a fixed schemas provided by the Record-Store.

## Managed KeyValue

Do you need a Key/Value experience? You can easily use this dedicated gRPC endpoint:

```grpc
service ManagedKV {
  rpc put(KeyValue) returns (EmptyResponse);
  rpc delete(DeleteRequest) returns (EmptyResponse);
  rpc scan(ScanRequest) returns (stream KeyValue);
}

message EmptyResponse {}

message DeleteRequest {
  bytes key_to_delete = 1;
}

message ScanRequest {
  bytes start_key = 1;
  bytes end_key = 2;
}

message KeyValue {
  bytes key = 1;
  bytes value = 2;
}
```
