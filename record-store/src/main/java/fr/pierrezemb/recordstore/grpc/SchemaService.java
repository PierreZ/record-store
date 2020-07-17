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
package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.protobuf.ProtobufReflectionUtil;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaService extends SchemaServiceGrpc.SchemaServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(SchemaService.class);
  private final RecordLayer recordLayer;

  public SchemaService(RecordLayer recordLayer) {
    this.recordLayer = recordLayer;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void get(RecordStoreProtocol.GetSchemaRequest request, StreamObserver<RecordStoreProtocol.GetSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    try {

      List<RecordStoreProtocol.IndexDescription> indexes = recordLayer.getIndexes(tenantID, recordSpace);

      RecordMetaData metadataStore = recordLayer.getSchema(tenantID, recordSpace);

      List<RecordStoreProtocol.SchemaDescription> records =
        ImmutableMap.of(request.getRecordTypeName(), metadataStore.getRecordMetaData().getRecordType(request.getRecordTypeName()))
          .entrySet()
          .stream()
          .map(e -> RecordStoreProtocol.SchemaDescription.newBuilder()
            .setName(e.getKey())
            .addAllIndexes(indexes)
            .addPrimaryKeyField(e.getValue().getPrimaryKey().toKeyExpression().getField().getFieldName())
            .setSchema(ProtobufReflectionUtil.protoFileDescriptorSet(e.getValue().getDescriptor()))
            .build())
          .collect(Collectors.toList());


      responseObserver.onNext(RecordStoreProtocol.GetSchemaResponse.newBuilder()
        .setSchemas(records.get(0))
        .setVersion(metadataStore.getRecordMetaData().getVersion())
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }


  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void upsert(RecordStoreProtocol.UpsertSchemaRequest request, StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    try {
      recordLayer.upsertSchema(tenantID, recordSpace, request.getSchema(), request.getRecordTypeIndexDefinitionsList());
    } catch (MetaDataException | Descriptors.DescriptorValidationException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }

    responseObserver.onNext(RecordStoreProtocol.EmptyResponse.newBuilder().build());
    responseObserver.onCompleted();
  }


  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void stat(RecordStoreProtocol.StatRequest request, StreamObserver<RecordStoreProtocol.StatResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    try {
      Tuple result = recordLayer.getCountAndCountUpdates(tenantID, recordSpace);
      responseObserver.onNext(RecordStoreProtocol.StatResponse.newBuilder()
        .setCount(result.getLong(0))
        .setCountUpdates(result.getLong(1))
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }
}
