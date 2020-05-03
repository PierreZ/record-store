package fr.pierrezemb.recordstore.grpc;

import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.gson.internal.bind.JsonTreeReader;
import com.google.protobuf.*;
import fr.pierrezemb.recordstore.fdb.RSKeySpace;
import fr.pierrezemb.recordstore.fdb.RSMetaDataStore;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.RecordQueryGenerator;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class RecordService extends RecordServiceGrpc.RecordServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(RecordService.class);
  private final FDBDatabase db;
  private final FDBStoreTimer timer;

  public RecordService(FDBDatabase db, FDBStoreTimer fdbStoreTimer) {
    this.db = db;
    this.timer = fdbStoreTimer;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void put(RecordStoreProtocol.PutRecordRequest request, StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {

      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RSKeySpace.getDataKeySpacePath(tenantID, container))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);

      Descriptors.Descriptor descriptor = metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName(request.getTable());

      DynamicMessage msg = DynamicMessage
        .parseFrom(
          descriptor,
          request.getMessage());

      r.saveRecord(msg);
      context.commit();

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

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {

      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RSKeySpace.getDataKeySpacePath(tenantID, container))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);
      RecordQuery query = RecordQueryGenerator.generate(request);

      this.executeQuery(r, query, tenantID, container)
        .map(e -> {
          if (log.isTraceEnabled()) {
            log.trace("found record '{}' from {}/{}", e.getPrimaryKey(), tenantID, container);
          }
          return e;
        })
        .map(FDBRecord::getRecord)
        .map(Message::toByteString)
        .forEach(e -> responseObserver.onNext(RecordStoreProtocol.QueryResponse.newBuilder().setRecord(e).build()))
        .join();

      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }

  private RecordCursor<FDBQueriedRecord<Message>> executeQuery(FDBRecordStore r, RecordQuery query, String tenantID, String container) {
    // TODO: handle errors instead of throwing null
    if (query == null) {
      log.error("query is null, skipping");
      throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("bad query"));
    }

    RecordQueryPlan plan = r.planQuery(query);
    log.info("running query for {}/{}: '{}'", tenantID, container, plan);

    ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
      .setIsolationLevel(IsolationLevel.SERIALIZABLE)
      .setDefaultCursorStreamingMode(CursorStreamingMode.ITERATOR); // either WANT_ALL OR streaming mode

    executeProperties.setScannedBytesLimit(1_000_000); // 1MB

    return r.executeQuery(query, null, executeProperties.build());
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void delete(RecordStoreProtocol.DeleteRecordRequest request, StreamObserver<RecordStoreProtocol.DeleteRecordResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();
    String container = GrpcContextKeys.getContainerOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {

      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID, container);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RSKeySpace.getDataKeySpacePath(tenantID, container))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);

      Integer count = 0;

      if (request.getDeleteAll()) {
        r.deleteAllRecords();
      } else {
        // running delete on query Mode
        RecordQuery query = RecordQueryGenerator.generate(request);


        count = this.executeQuery(r, query, tenantID, container)
          .map(e -> {
            if (log.isTraceEnabled()) {
              log.trace("deleting {} from {}/{}", e.getPrimaryKey(), tenantID, container);
            }
            return e;
          })
          .map(e -> r.deleteRecord(e.getPrimaryKey()))
          .getCount().join();
      }
      context.commit();

      responseObserver.onNext(RecordStoreProtocol.DeleteRecordResponse.newBuilder()
        .setDeletedCount(count.longValue())
        .build());
      responseObserver.onCompleted();
    } catch (RuntimeException e) {
      log.error(e.getMessage());
      throw new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage()));
    }
  }
}
