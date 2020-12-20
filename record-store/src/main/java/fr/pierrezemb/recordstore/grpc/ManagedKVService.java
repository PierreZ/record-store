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
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.managed.kv.ManagedKVGrpc;
import fr.pierrezemb.recordstore.proto.managed.kv.ManagedKVProto;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedKVService extends ManagedKVGrpc.ManagedKVImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(ManagedKVService.class);
  private static final String MANAGED_KV_NAME = "managedKV";
  private final RecordLayer recordLayer;
  private final RecordMetaData recordMetaData;

  public ManagedKVService(RecordLayer recordLayer) {
    this.recordLayer = recordLayer;
    RecordMetaDataBuilder recordMetaDataBuilder = RecordMetaData.newBuilder().setRecords(ManagedKVProto.getDescriptor());
    recordMetaDataBuilder.getRecordType("KeyValue").setPrimaryKey(Key.Expressions.field("key"));
    this.recordMetaData = recordMetaDataBuilder.build();
  }

  @Override
  public void put(ManagedKVProto.KeyValue request, StreamObserver<ManagedKVProto.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    try {
      this.recordLayer.putRecord(tenantID, MANAGED_KV_NAME,recordSpace, this.recordMetaData, request);
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }

    responseObserver.onNext(ManagedKVProto.EmptyResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void delete(ManagedKVProto.DeleteRequest request, StreamObserver<ManagedKVProto.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    Tuple primaryKey = Tuple.from(request.getKeyToDelete().toByteArray());

    try {
      boolean deleted = this.recordLayer.deleteRecord(tenantID, MANAGED_KV_NAME, recordSpace, this.recordMetaData, primaryKey);
      LOGGER.debug("delete({})={}", primaryKey.toString(), deleted);
      responseObserver.onNext(ManagedKVProto.EmptyResponse.newBuilder().build());
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
    responseObserver.onCompleted();
  }


  @Override
  public void scan(ManagedKVProto.ScanRequest request, StreamObserver<ManagedKVProto.KeyValue> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    RecordQuery query = RecordQuery
      .newBuilder()
      .setRecordType("KeyValue")
      .setFilter(
        request.getEndKey().isEmpty() ?
          Query.field("key").equalsValue(request.getStartKey().toByteArray())
          :
          Query.and(
            Query.field("key").greaterThanOrEquals(request.getStartKey().toByteArray()),
            Query.field("key").lessThanOrEquals(request.getEndKey().toByteArray())
          )
      )
      .build();

    try {
      this.recordLayer.scanRecords(tenantID, MANAGED_KV_NAME, recordSpace, this.recordMetaData, query)
        .stream()
        .map(queriedRecord -> ManagedKVProto.KeyValue.newBuilder().mergeFrom(queriedRecord).build())
        .forEach(responseObserver::onNext);
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
    responseObserver.onCompleted();
  }

}
