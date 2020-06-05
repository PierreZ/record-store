package fr.pierrezemb.recordstore.fdb;

import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializerJCE;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import fr.pierrezemb.recordstore.fdb.metrics.FDBMetricsStoreTimer;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fr.pierrezemb.recordstore.fdb.UniversalIndexes.COUNT_INDEX;
import static fr.pierrezemb.recordstore.fdb.UniversalIndexes.COUNT_UPDATES_INDEX;
import static fr.pierrezemb.recordstore.fdb.UniversalIndexes.INDEX_COUNT_AGGREGATE_FUNCTION;
import static fr.pierrezemb.recordstore.fdb.UniversalIndexes.INDEX_COUNT_UPDATES_AGGREGATE_FUNCTION;

public class RecordLayer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordLayer.class);
  private final FDBDatabase db;
  private final FDBMetricsStoreTimer timer;
  private final SecretKey defaultKey;

  public RecordLayer(String clusterFilePath, boolean enableMetrics, SecretKey key) throws InterruptedException, ExecutionException, TimeoutException {
    db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);
    db.performNoOpAsync().get(2, TimeUnit.SECONDS);
    System.out.println("connected to FDB!");
    timer = new FDBMetricsStoreTimer(enableMetrics);
    defaultKey = key;
  }

  /**
   * List all containers for a tenant
   */
  public List<String> listContainers(String tenantID) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    KeySpacePath tenantKeySpace = RecordStoreKeySpace.getApplicationKeySpacePath(tenantID);
    List<ResolvedKeySpacePath> containers = tenantKeySpace
      .listSubdirectory(context, "container", ScanProperties.FORWARD_SCAN);
    return containers.stream()
      .map(e -> e.getResolvedValue().toString())
      .collect(Collectors.toList());
  }

  /**
   * delete a container for a tenant
   */
  public void deleteContainer(String tenantID, String container) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBRecordStore.deleteStore(context, RecordStoreKeySpace.getDataKeySpacePath(tenantID, container));
    FDBRecordStore.deleteStore(context, RecordStoreKeySpace.getMetaDataKeySpacePath(tenantID, container));
    context.commit();
  }

  /**
   * get schema for a tenant and a container
   */
  public RecordMetaData getSchema(String tenantID, String container) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

    List<RecordStoreProtocol.IndexDescription> indexes = metaDataStore.getRecordMetaData().getAllIndexes().stream()
      .filter(e -> !e.getName().startsWith("global"))
      .map(e ->
        RecordStoreProtocol.IndexDescription.newBuilder()
          .build()
      ).collect(Collectors.toList());

    return metaDataStore.getRecordMetaData();
  }

  public List<RecordStoreProtocol.IndexDescription> getIndexes(String tenantID, String container) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

    return metaDataStore.getRecordMetaData().getAllIndexes().stream()
      .filter(e -> !e.getName().startsWith("global"))
      .map(e ->
        RecordStoreProtocol.IndexDescription.newBuilder()
          .build()
      ).collect(Collectors.toList());
  }

  public void upsertSchema(String tenantID, String container, DescriptorProtos.FileDescriptorSet schema, List<RecordStoreProtocol.IndexSchemaRequest> indexes) throws Descriptors.DescriptorValidationException {

    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

    RecordMetaData oldMetaData = null;
    int version = 0;
    try {
      oldMetaData = metaDataStore.getRecordMetaData();
      LOGGER.debug("metadata for {}:{} is in version {}", tenantID, container, oldMetaData.getVersion());
      version = oldMetaData.getVersion() + 1;
    } catch (FDBMetaDataStore.MissingMetaDataException e) {
      LOGGER.info("missing metadata, creating one");
    }

    RecordMetaData newRecordMetaData = createRecordMetaData(schema, indexes, version, oldMetaData);

    // handling upgrade
    if (null != oldMetaData) {
      MetaDataEvolutionValidator metaDataEvolutionValidator = MetaDataEvolutionValidator.newBuilder()
        .setAllowIndexRebuilds(true)
        .setAllowMissingFormerIndexNames(false)
        .build();

      metaDataEvolutionValidator.validate(oldMetaData, newRecordMetaData);
    }

    // and save it
    metaDataStore.saveRecordMetaData(newRecordMetaData.getRecordMetaData().toProto());

    context.commit();

  }

  private RecordMetaData createRecordMetaData(DescriptorProtos.FileDescriptorSet schema, List<RecordStoreProtocol.IndexSchemaRequest> indexes, int version, RecordMetaData oldMetadata) throws Descriptors.DescriptorValidationException {

    // retrieving protobuf descriptor
    RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder();

    for (DescriptorProtos.FileDescriptorProto fdp : schema.getFileList()) {
      Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(fdp, new Descriptors.FileDescriptor[]{});
      // updating schema
      metadataBuilder.setRecords(fd);
    }

    // set options
    metadataBuilder.setVersion(version);
    metadataBuilder.setStoreRecordVersions(true);
    metadataBuilder.setSplitLongRecords(true);

    HashSet<Index> oldIndexes = oldMetadata != null ?
      new HashSet<>(oldMetadata.getAllIndexes()) :
      new HashSet<>();
    HashSet<String> oldIndexesNames = new HashSet<>();

    // add old indexes
    for (Index index : oldIndexes) {
      LOGGER.trace("adding old index {}", index.getName());
      oldIndexesNames.add(index.getName());
      if (index.getName().equals(UniversalIndexes.COUNT_INDEX_NAME) || index.getName().equals(UniversalIndexes.COUNT_UPDATES_INDEX_NAME)) {
        metadataBuilder.addUniversalIndex(index);
      } else {
        // we need to retrieve the record
        String[] idxNameSplitted = index.getName().split("_", 4);
        if (idxNameSplitted.length != 4) {
          LOGGER.warn("strange idx name: '{}', skipping", index.getName());
          continue;
        }
        LOGGER.trace("adding already known index {}", idxNameSplitted[0]);
        metadataBuilder.addIndex(idxNameSplitted[0], index);
      }
    }

    // we need to loop through all index requests
    for (RecordStoreProtocol.IndexSchemaRequest idxRequest : indexes) {
      LOGGER.trace("adding indexes for {}", idxRequest.getName());
      // add new indexes
      for (RecordStoreProtocol.IndexDefinition indexDefinition : idxRequest.getIndexDefinitionsList()) {

        String indexName = generateIndexName(idxRequest.getName(), indexDefinition);
        Index index = createIndex(indexDefinition, indexName);
        if (!oldIndexesNames.contains(indexName)) {
          LOGGER.trace("adding new index {} of type {}", indexName, indexDefinition.getIndexType());
          metadataBuilder.addIndex(idxRequest.getName(), index);
        }
      }
      // set primary key
      metadataBuilder.getRecordType(idxRequest.getName())
        .setPrimaryKey(buildPrimaryKeyExpression(idxRequest.getPrimaryKeyFieldsList()));
    }


    if (oldMetadata == null) {
      metadataBuilder.addUniversalIndex(COUNT_INDEX);
      metadataBuilder.addUniversalIndex(COUNT_UPDATES_INDEX);
    }

    return metadataBuilder.build();
  }

  private String generateIndexName(String name, RecordStoreProtocol.IndexDefinition indexDefinition) {
    if (!indexDefinition.hasNestedIndex()) {
      return name + "_idx_" + indexDefinition.getField() + "_" + indexDefinition.getIndexType().toString();
    }
    return name + "_idx_" + indexDefinition.getField() + "_nested_" + generateIndexName(name, indexDefinition.getNestedIndex());
  }

  private Index createIndex(RecordStoreProtocol.IndexDefinition indexDefinition, String indexName) {
    Index index = null;

    if (indexDefinition.hasNestedIndex()) {
      return new Index(
        indexName,
        Key.Expressions.field(indexDefinition.getField(), getFanType(indexDefinition.getFanType()))
          .nest(createKeyExpressionFromIndexDefinition(indexDefinition.getNestedIndex())));
    }

    switch (indexDefinition.getIndexType()) {
      case VALUE:
        index = new Index(
          indexName,
          Key.Expressions.field(indexDefinition.getField(), getFanType(indexDefinition.getFanType())),
          IndexTypes.VALUE);
        break;
      // https://github.com/FoundationDB/fdb-record-layer/blob/e70d3f9b5cec1cf37b6f540d4e673059f2a628ab/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/provider/foundationdb/indexes/TextIndexMaintainer.java#L81-L93
      case TEXT_DEFAULT_TOKENIZER:
        index = new Index(
          indexName,
          Key.Expressions.field(indexDefinition.getField(), getFanType(indexDefinition.getFanType())),
          IndexTypes.TEXT);
        break;
      case VERSION:
        index = new Index(
          indexName,
          VersionKeyExpression.VERSION,
          IndexTypes.VERSION);
        break;
      case MAP_KEYS:
        index = new Index(
          indexName,
          Key.Expressions.mapKeys(indexDefinition.getField())
        );
        break;
      case MAP_VALUES:
        index = new Index(
          indexName,
          Key.Expressions.mapValues(indexDefinition.getField())
        );
        break;
      case MAP_KEYS_AND_VALUES:
        index = new Index(
          indexName,
          Key.Expressions.mapKeyValues(indexDefinition.getField())
        );
        break;
      case UNRECOGNIZED:
        return null;
    }
    return index;
  }

  private KeyExpression createKeyExpressionFromIndexDefinition(RecordStoreProtocol.IndexDefinition nestedIndex) {
    return Key.Expressions.field(nestedIndex.getField(), getFanType(nestedIndex.getFanType()));
  }

  private KeyExpression.FanType getFanType(RecordStoreProtocol.FanType fanType) {
    if (fanType == null) {
      return KeyExpression.FanType.None;
    }

    switch (fanType) {
      case FAN_CONCATENATE:
        return KeyExpression.FanType.Concatenate;
      case FAN_OUT:
        return KeyExpression.FanType.FanOut;
    }
    return KeyExpression.FanType.None;
  }

  private KeyExpression buildPrimaryKeyExpression(List<String> primaryKeyFields) {
    List<KeyExpression> keyExpressions = primaryKeyFields
      .stream()
      .map(Key.Expressions::field)
      .collect(Collectors.toList());

    // adding the recordType in the key expressions. Following advices from
    // https://forums.foundationdb.org/t/split-long-record-causes-conflict-with-other-record/2160/2?u=pierrez
    keyExpressions.add(0, Key.Expressions.recordType());

    return Key.Expressions.concat(keyExpressions);
  }

  public Tuple getCountAndCountUpdates(String tenantID, String container) {
    return getCountAndCountUpdates(tenantID, container, defaultKey);
  }

  public Tuple getCountAndCountUpdates(String tenantID, String container, SecretKey key) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, key, tenantID, container);

    CompletableFuture<Tuple> countFuture = r.evaluateAggregateFunction(
      EvaluationContext.EMPTY,
      Collections.emptyList(),
      INDEX_COUNT_AGGREGATE_FUNCTION,
      TupleRange.ALL,
      IsolationLevel.SERIALIZABLE);

    CompletableFuture<Tuple> updateFuture = r.evaluateAggregateFunction(
      EvaluationContext.EMPTY,
      Collections.emptyList(),
      INDEX_COUNT_UPDATES_AGGREGATE_FUNCTION,
      TupleRange.ALL,
      IsolationLevel.SERIALIZABLE);

    return countFuture.thenCombine(updateFuture, (count, update)
      -> Tuple.from(count.getLong(0), update.getLong(0))).join();
  }

  public void putRecord(String tenantID, String container, String table, byte[] record, SecretKey customKey) throws InvalidProtocolBufferException {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

    Descriptors.Descriptor descriptor = metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName(table);

    if (descriptor == null) {
      throw new RuntimeException("cannot find descriptor for table " + table);
    }

    FDBRecordStore r = createFDBRecordStore(context, metaDataStore, customKey, tenantID, container);
    DynamicMessage msg = DynamicMessage.parseFrom(descriptor, record);

    r.saveRecord(msg);
    context.commit();
  }


  public void putRecord(String tenantID, String container, String table, byte[] record) throws InvalidProtocolBufferException {
    putRecord(tenantID, container, table, record, defaultKey);
  }

  public List<Message> queryRecords(String tenantID, String container, RecordQuery query) {
    return queryRecords(tenantID, container, query, defaultKey);
  }

  public List<Message> queryRecords(String tenantID, String container, RecordQuery query, SecretKey key) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, key, tenantID, container);

    return this.executeQuery(r, query, tenantID, container)
      .map(e -> {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("found record '{}' from {}/{}", e.getPrimaryKey(), tenantID, container);
        }
        return e;
      })
      .map(FDBRecord::getRecord)
      .asList()
      .join();
  }

  public void queryRecords(String tenantID, String container, RecordQuery query, StreamObserver<RecordStoreProtocol.QueryResponse> responseObserver) {
    queryRecords(tenantID, container, query, defaultKey, responseObserver);
  }

  public void queryRecords(String tenantID, String container, RecordQuery query, SecretKey key, StreamObserver<RecordStoreProtocol.QueryResponse> responseObserver) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, key, tenantID, container);

    this.executeQuery(r, query, tenantID, container)
      .map(e -> {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("found record '{}' from {}/{}", e.getPrimaryKey(), tenantID, container);
        }
        return e;
      })
      .map(FDBRecord::getRecord)
      .map(Message::toByteString)
      .forEach(e -> responseObserver.onNext(RecordStoreProtocol.QueryResponse.newBuilder().setRecord(e).build()))
      .join();
  }

  public void queryRecords(String tenantID, String container, RecordQuery query, Promise<List<Map<String, Object>>> future) {
    queryRecords(tenantID, container, query, defaultKey, future);
  }

  public void queryRecords(String tenantID, String container, RecordQuery query, SecretKey encryptionKey, Promise<List<Map<String, Object>>> future) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, encryptionKey, tenantID, container);

    Descriptors.Descriptor descriptor = r.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("Person");

    List<Map<String, Object>> result = null;
    try {
      result = this.executeQuery(r, query, tenantID, container)
        .map(e -> {
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("found record '{}' from {}/{}", e.getPrimaryKey(), tenantID, container);
          }
          return e;
        })
        .map(queriedRecord -> {
          try {
            return DynamicMessage.parseFrom(descriptor, queriedRecord.getStoredRecord().getRecord().toByteArray());
          } catch (InvalidProtocolBufferException e) {
            return null;
          }
        })
        .filter(Objects::nonNull)

        // TODO: can we replace `graphql.schema.PropertyDataFetcher` to avoid casting things as an HashMap?
        .map(dynamicMessage -> {
          Map<String, Object> results = new HashMap<>();
          dynamicMessage.getAllFields().forEach((key, value) -> results.put(key.getName(), value));
          return results;
        })
        .asList().get();
      future.complete(result);
    } catch (InterruptedException | ExecutionException e) {
      future.fail(e);
    }
  }

  private RecordCursor<FDBQueriedRecord<Message>> executeQuery(FDBRecordStore r, RecordQuery query, String tenantID, String container) {
    // TODO: handle errors instead of throwing null
    if (query == null) {
      LOGGER.error("query is null, skipping");
      throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("bad query"));
    }

    LOGGER.info(query.toString());

    RecordQueryPlan plan = r.planQuery(query);
    LOGGER.info("running query for {}/{}: '{}'", tenantID, container, plan);

    ExecuteProperties.Builder executeProperties = ExecuteProperties.newBuilder()
      .setIsolationLevel(IsolationLevel.SERIALIZABLE)
      .setDefaultCursorStreamingMode(CursorStreamingMode.ITERATOR); // either WANT_ALL OR streaming mode

    executeProperties.setScannedBytesLimit(1_000_000); // 1MB

    return r.executeQuery(query, null, executeProperties.build());
  }

  public long deleteAllRecords(String tenantID, String container) {
    return deleteAllRecords(tenantID, container, defaultKey);
  }

  public long deleteAllRecords(String tenantID, String container, SecretKey key) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, key, tenantID, container);
    r.deleteAllRecords();
    // TODO: return count of records with the call stats
    return 0;
  }

  public long deleteRecords(String tenantID, String container, RecordQuery query) {
    return deleteRecords(tenantID, container, query, defaultKey);
  }

  public long deleteRecords(String tenantID, String container, RecordQuery query, SecretKey key) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, key, tenantID, container);

    Integer count = this.executeQuery(r, query, tenantID, container)
      .map(e -> {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("deleting {} from {}/{}", e.getPrimaryKey(), tenantID, container);
        }
        return e;
      })
      .map(e -> r.deleteRecord(e.getPrimaryKey()))
      .getCount().join();
    context.commit();
    return count;
  }

  public String getQueryPlan(String tenantID, String container, RecordQuery query) {
    return getQueryPlan(tenantID, container, query, defaultKey);
  }

  public String getQueryPlan(String tenantID, String container, RecordQuery query, SecretKey key) {
    FDBRecordContext context = db.openContext(Collections.singletonMap("tenant", tenantID), timer);
    FDBMetaDataStore metadataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);
    FDBRecordStore r = createFDBRecordStore(context, metadataStore, key, tenantID, container);
    return r.planQuery(query).toString();
  }

  private FDBRecordStore createFDBRecordStore(FDBRecordContext context, FDBMetaDataStore metaDataStore, SecretKey key, String tenantID, String container) {

    TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
      .setEncryptWhenSerializing(true)
      .setCompressWhenSerializing(true)
      .setEncryptionKey(key)
      .build();

    // Helper func
    Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
      .setMetaDataProvider(metaDataStore)
      .setContext(context)
      .setSerializer(serializer)
      .setKeySpacePath(RecordStoreKeySpace.getDataKeySpacePath(tenantID, container))
      .createOrOpen();

    return recordStoreProvider.apply(context);
  }
}
