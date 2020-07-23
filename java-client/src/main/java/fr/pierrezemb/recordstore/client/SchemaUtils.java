/**
 * Copyright 2020 Pierre Zemb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.pierrezemb.recordstore.client;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.protobuf.ProtobufReflectionUtil;

import java.util.Collections;
import java.util.List;

/**
 * A Utils class that can be used to easily generate UpsertSchema requests
 */
public class SchemaUtils {
  public static RecordStoreProtocol.UpsertSchemaRequest createSchemaRequest(
    Descriptors.Descriptor descriptor,
    List<RecordStoreProtocol.RecordTypeIndexDefinition> indexDefinitionList) {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(descriptor);

    return RecordStoreProtocol.UpsertSchemaRequest.newBuilder()
      .setSchema(dependencies)
      .addAllRecordTypeIndexDefinitions(indexDefinitionList)
      .build();
  }

  public static RecordStoreProtocol.UpsertSchemaRequest createSchemaRequest(
    Descriptors.Descriptor descriptor,
    String recordTypeName,
    String primaryKey,
    String indexField, RecordStoreProtocol.IndexType indexType) {
    return createSchemaRequest(descriptor, Collections.singletonList(
      createIndex(recordTypeName, primaryKey, Collections.singletonList(createIndexDefinition(indexField, indexType)))
    ));
  }

  public static RecordStoreProtocol.UpsertSchemaRequest createSchemaRequest(
    Descriptors.Descriptor descriptor,
    RecordStoreProtocol.RecordTypeIndexDefinition indexDefinition) {
    return createSchemaRequest(descriptor, Collections.singletonList(indexDefinition));
  }

  public static RecordStoreProtocol.RecordTypeIndexDefinition createIndex(String name, String primaryKeyField, RecordStoreProtocol.IndexDefinition indexDefinition) {
    return createIndex(name, Collections.singletonList(primaryKeyField), Collections.singletonList(indexDefinition));
  }

  public static RecordStoreProtocol.RecordTypeIndexDefinition createIndex(String name, String primaryKeyField, List<RecordStoreProtocol.IndexDefinition> indexDefinitions) {
    return createIndex(name, Collections.singletonList(primaryKeyField), indexDefinitions);
  }


  public static RecordStoreProtocol.RecordTypeIndexDefinition createIndex(String name, List<String> primaryKeyFields, List<RecordStoreProtocol.IndexDefinition> indexDefinitions) {
    return RecordStoreProtocol.RecordTypeIndexDefinition.newBuilder()
      .setName(name)
      .addAllIndexDefinitions(indexDefinitions)
      .addAllPrimaryKeyFields(primaryKeyFields)
      .build();
  }

  public static RecordStoreProtocol.IndexDefinition createIndexDefinition(String field, RecordStoreProtocol.IndexType indexType) {
    return RecordStoreProtocol.IndexDefinition.newBuilder()
      .setField(field)
      .setIndexType(indexType)
      .build();
  }

  public static RecordStoreProtocol.IndexDefinition createIndexDefinition(String field, RecordStoreProtocol.IndexType indexType, RecordStoreProtocol.FanType fanType) {
    return RecordStoreProtocol.IndexDefinition.newBuilder()
      .setField(field)
      .setIndexType(indexType)
      .setFanType(fanType)
      .build();
  }
}
