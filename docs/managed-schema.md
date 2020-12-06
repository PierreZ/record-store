---
title: Managed schemas
---

Record-Store is great to handle customers defined schema, but sometimes you may need to store something simple.

We are now providing dedicated `recordSpace` where the schema is directly handled by the Record-Store, lowering the difficulty to use the Record-Store. Under the hood, you will still open a `recordSpace`, but you will not be able to manipulate the schema.

## Managed KeyValue

You want to use a simple key/value? We got you covered! We even offer a simplified gRPC service:

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
