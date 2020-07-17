---
title: Writing a Record-Store client
---

# Writing a Record-Store client

## Requirement

We are using [gRPC](https://grpc.io/) as the underlying RPC framework, so you should find a gRPC library for your language first. You only need to generate the client part.

## Terminology

* `tenant` is your account
* `RecordSpace` is like a virtual SQL table, it is a logical place where you have `schemas` and `data`
* `RecordType` is the name of the message structure defined in protobuf
* `Record` is a protobuf message

## Auth

We are using gRPC headers to pass:

* the container (The `RecordSpace` key in header)
* the tenant (The `Tenant` key in header)
* the token (The `Authorization` key in header) using the Bearer convention.

It is not embedded within the proto as we want to be able to proxify cnx to the right cluster storage.

## Implementation details

For all the following information, we are going to use this protobuf definition as an example:

```protobuf
syntax = "proto3";

message User {
  int64 id = 1;
  string name = 2;
  string email = 3;
}

// this is mandatory for now, you really need to use the name `RecordTypeUnion`
// and include all the message that you are using
message RecordTypeUnion {
  User _User = 1;
}
```

### Schema

#### FileDescriptorSet

When opening a `RecordSpace`, first thing is to upsert a schema. You need to use the `SchemaService` with the `Upsert` method. It is safe to think that users will make an UpsertSchema after starting the cnx.

You need to pass the following struct:

```protobuf
message UpsertSchemaRequest {
  // the schema used
  google.protobuf.FileDescriptorSet schema = 1;
  // list of index definitions
  repeated RecordTypeIndexDefinition record_type_index_definitions = 2;
  // optional field, used if you need to use another Union-type
  string record_type_union_name = 3;
}
```

`google.protobuf.FileDescriptorSet` are a special type provided by the google's gRPC standard library. It allows someone to serialize proto files in protobuf. You should be able to create them from the `Descriptor` type that is generated with the client. Here's some example in other languages:

java:
```java
  public static FileDescriptorSet protoFileDescriptorSet(Descriptor descriptor) {
    Set<FileDescriptor> descriptors = new HashSet<>();
    descriptors.add(descriptor.getFile());
    addDependenciesRecursively(descriptors, descriptor.getFile());

    Builder fileDescriptorSet = FileDescriptorSet.newBuilder();
    for (FileDescriptor d : descriptors) {
      fileDescriptorSet.addFile(d.toProto());
    }
    return fileDescriptorSet.build();
  }

  private static void addDependenciesRecursively(
    Set<FileDescriptor> visited, FileDescriptor descriptor) {
    for (FileDescriptor dependency : descriptor.getDependencies()) {
      if (visited.add(dependency)) {
        addDependenciesRecursively(visited, dependency.getFile());
      }
    }
  }
```

go:
```go
func CreateSchemaFromFileDescriptor(input protoreflect.FileDescriptor) (*descriptorpb.FileDescriptorSet, error) {

	got := protodesc.ToFileDescriptorProto(input)
	return &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{got},
	}, nil
}
```

#### record_type_index_definitions

You can define multiple indexes for your Schema, and you can define multiple schema for one RecordSpace.

```protobuf
message RecordTypeIndexDefinition {
  // name of the recordType
  string name = 1;
  // fields used for creating the primary key
  repeated string primary_key_fields = 2;
  // list of index definitions
  repeated IndexDefinition index_definitions = 3;
}
```

Using our example, we need to set:

* name to `User`
* primary key to `id` (we can add multiple fields to create a tuple)
* and a list of index definitions.

Index definitions allow query on field. You can add them by  using these structures:

``` protobuf
message IndexDefinition {
  // field of the index
  string field = 1;
  // Type of the index
  IndexType index_type = 2;
  // if it is a repeated type, how we should fan it
  FanType fan_type = 3;
  // the nestedIndex should point within the nested struct
  // the nested index should only have field and fan_type set.
  IndexDefinition nestedIndex = 4;
}

enum IndexType {
  // Index the value
  VALUE = 0;
  // Index sentences by splitting on spaces
  TEXT_DEFAULT_TOKENIZER = 1;
  // Index the version of a field
  VERSION = 2;
  // Index the key of a map
  MAP_KEYS = 3;
  // Index the values of a map
  MAP_VALUES = 4;
  // Index the whole map
  MAP_KEYS_AND_VALUES = 5;
}
```

In our case, we could index the `mail` field using a `VALUE` index.

### Push data

Now that we have a schema and a container. Let's push a `User`! You need the `RecordService` and the `Put` rpc call.

Pushing a data is simple, you only need to create this message

```protobuf
message PutRecordRequest {
  // name of the recordType
  string record_type_name = 1;
  // serialized version of the record
  bytes message = 2;
}
```

RecordTypeName is the name of the record Type (here `User`) and message are the serialized version of your message. As we just sent the schema, we can just send bytes over the network.

### Query data

Let's query our data! You need to use the `rpc Query (QueryRequest) returns (stream QueryResponse);`

```protobuf
message QueryRequest {
  // name of the queryType
  string record_type_name = 1;
  // the query itself
  QueryFilterNode filter = 2;
// the other fields are optionals
}
```

As always, record_type_name will be `User`. Let's go through the filter:

```protobuf
message QueryFilterNode {
  oneof Content {
    QueryFilterFieldNode field_node = 1;
    QueryFilterAndNode and_node = 2;
    QueryFilterOrNode or_node = 3;
    // apply operation on a indexed map
    QueryFilterMapNode map_node = 4;
  }
}
```

QueryFilterNode is a tree-like representation of a query, that allow complex queries. We are targeting a developer experience like this:

```java
Query.and(
	field("id").greaterThan(1),
	field("email").equals("contact@example.org")
)
```

We recommend wrap the protobuf builder to allow nice queries.

the easiest query is to define a `field_node`. Here's an example

```
QueryFilterFieldNode.field = "email"
QueryFilterFieldNode.operation = START_WITH
QueryFilterFieldNode.value.string_value = "contact@"
```

You have a lot of operations to wrap, sorry:

```protobuf
enum FilterOperation {
  // valid for field query
  GREATER_THAN_OR_EQUALS = 0;
  LESS_THAN_OR_EQUALS = 1;
  GREATER_THAN = 2;
  LESS_THAN = 3;
  START_WITH = 4;
  IS_EMPTY = 5;
  IS_NULL = 6;
  EQUALS = 7;
  NOT_EQUALS = 8;
  NOT_NULL = 9;
  MATCHES = 10;

  // valid for text queries
  TEXT_CONTAINS_ANY = 11;
  TEXT_CONTAINS_ALL = 12;
}

```

Also, you need to set the exact same type as the one used in the protobuf in the FieldNode, hence the oneof value type:

```protobuf
message QueryFilterFieldNode {
  string field = 1;
  FilterOperation operation = 2;
  // set this to true if your field is repeated
  bool is_field_defined_as_repeated = 3;
  oneof value {
    string string_value = 11;
    int32 int32_value = 12;
    int64 int64_value = 13;
    float float_value = 14;
    uint32 uint32_value = 15;
    uint64 uint64_value = 16;
    sint32 sint32_value = 17;
    sint64 sint64_value = 18;
    double double_value = 19;
    bool bool_value = 20;
    bytes bytes_value = 21;
    QueryFilterFieldNode fieldNode = 22;
  }

  repeated string tokens = 40;
}
```
