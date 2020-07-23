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

import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.query.RecordQuery;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.query.GrpcQueryGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordService extends RecordServiceGrpc.RecordServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(RecordService.class);
  private final RecordLayer recordLayer;

  public RecordService(RecordLayer recordLayer) {
    this.recordLayer = recordLayer;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void put(RecordStoreProtocol.PutRecordRequest request, StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    try {
      this.recordLayer.putRecord(tenantID, recordSpace, request.getRecordTypeName(), request.getMessage().toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("could not parse Protobuf: " + e.getMessage()));
    } catch (RuntimeException e) {
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
  public void query(RecordStoreProtocol.QueryRequest request, StreamObserver<RecordStoreProtocol.QueryResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();
    RecordQuery query = GrpcQueryGenerator.generate(request);
    IsolationLevel isolationLevel = request
      .getQueryIsolationLevel().equals(RecordStoreProtocol.QueryIsolationLevel.SERIALIZABLE) ?
      IsolationLevel.SERIALIZABLE : IsolationLevel.SNAPSHOT;

    try {
      this.recordLayer.queryRecords(tenantID, recordSpace, query, isolationLevel, responseObserver);
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
  public void delete(RecordStoreProtocol.DeleteRecordRequest request, StreamObserver<RecordStoreProtocol.DeleteRecordResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();

    long count = 0L;
    try {
      if (request.getDeleteAll()) {
        count = this.recordLayer.deleteAllRecords(tenantID, recordSpace);
      } else {
        RecordQuery query = GrpcQueryGenerator.generate(request);
        count = this.recordLayer.deleteRecords(tenantID, recordSpace, query);
      }

      responseObserver.onNext(RecordStoreProtocol.DeleteRecordResponse.newBuilder()
        .setDeletedCount(count)
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }

  @Override
  public void getQueryPlan(RecordStoreProtocol.QueryRequest request, StreamObserver<RecordStoreProtocol.GetQueryPlanResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String recordSpace = GrpcContextKeys.getContainerOrFail();
    RecordQuery query = GrpcQueryGenerator.generate(request);

    try {
      String queryPlan = this.recordLayer.getQueryPlan(tenantID, recordSpace, query);
      responseObserver.onNext(RecordStoreProtocol.GetQueryPlanResponse.newBuilder()
        .setQueryPlan(query.toString())
        .setQueryPlan(queryPlan)
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }
}
