package fr.pierrezemb.recordstore.grpc;

import static fr.pierrezemb.recordstore.grpc.SchemaService.COUNT_INDEX;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.RecordQuery;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.fdb.RSKeySpace;
import fr.pierrezemb.recordstore.fdb.RSMetaDataStore;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.utils.RecordQueryGenerator;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class RecordService extends RecordServiceGrpc.RecordServiceImplBase {
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
  public void put(RecordStoreProtocol.PutRecordRequest request, StreamObserver<RecordStoreProtocol.CreateSchemaResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {

      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RSKeySpace.getDataKeySpacePath(tenantID))
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
      responseObserver.onError(e);
      responseObserver.onCompleted();
    }
    responseObserver.onNext(RecordStoreProtocol.CreateSchemaResponse.newBuilder().setResult(RecordStoreProtocol.Result.OK).build());
    responseObserver.onCompleted();
  }

  /**
   * Count is using directly the COUNT index
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void count(RecordStoreProtocol.CountRecordRequest request, StreamObserver<RecordStoreProtocol.CountRecordResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    IndexAggregateFunction function = new IndexAggregateFunction(
      FunctionNames.COUNT, COUNT_INDEX.getRootExpression(), COUNT_INDEX.getName());

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {
      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RSKeySpace.getDataKeySpacePath(tenantID))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);

      Long result = r.evaluateAggregateFunction(
        EvaluationContext.EMPTY,
        Collections.singletonList(request.getTable()),
        function,
        TupleRange.ALL,
        IsolationLevel.SERIALIZABLE)
        .thenApply(tuple -> tuple.getLong(0)).join();

      responseObserver.onNext(RecordStoreProtocol.CountRecordResponse.newBuilder().setSize(result).build());
      responseObserver.onCompleted();
    }
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void query(RecordStoreProtocol.QueryRequest request, StreamObserver<RecordStoreProtocol.QueryResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    try (FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer)) {

      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RSMetaDataStore.createMetadataStore(context, tenantID);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RSKeySpace.getDataKeySpacePath(tenantID))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);
      RecordQuery query = RecordQueryGenerator.generate(request);

      // TODO: we should tune SERIAL_EXECUTE
      List<ByteString> results = r.executeQuery(query, request.getContinuation().toByteArray(), ExecuteProperties.SERIAL_EXECUTE)
        .map(FDBRecord::getRecord)
        .map(Message::toByteString).asList().join();


      responseObserver.onNext(RecordStoreProtocol.QueryResponse.newBuilder()
        .setResult(RecordStoreProtocol.Result.OK)
        .addAllRecords(results)
        .build());
      responseObserver.onCompleted();
    }
  }
}
