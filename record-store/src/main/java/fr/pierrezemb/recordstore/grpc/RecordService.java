package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.query.RecordQuery;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.RecordQueryGenerator;
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
    String container = GrpcContextKeys.getContainerOrFail();

    try {
      this.recordLayer.putRecord(tenantID, container, request.getTable(), request.getMessage().toByteArray());
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
    String container = GrpcContextKeys.getContainerOrFail();
    RecordQuery query = RecordQueryGenerator.generate(request);

    try {
      this.recordLayer.queryRecordsWithObserver(tenantID, container, query, responseObserver);
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
    String container = GrpcContextKeys.getContainerOrFail();

    long count = 0L;
    try {
      if (request.getDeleteAll()) {
        count = this.recordLayer.deleteAllRecords(tenantID, container);
      } else {
        RecordQuery query = RecordQueryGenerator.generate(request);
        count = this.recordLayer.deleteRecords(tenantID, container, query);
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
    String container = GrpcContextKeys.getContainerOrFail();
    RecordQuery query = RecordQueryGenerator.generate(request);

    try {
      String queryPlan = this.recordLayer.getQueryPlan(tenantID, container, query);
      responseObserver.onNext(RecordStoreProtocol.GetQueryPlanResponse.newBuilder()
        .setQueryPlan(queryPlan)
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }
}
