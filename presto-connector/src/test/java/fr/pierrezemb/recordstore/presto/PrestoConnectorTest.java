package fr.pierrezemb.recordstore.presto;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.*;
import fr.pierrezemb.recordstore.fdb.RecordStoreKeySpace;
import fr.pierrezemb.recordstore.fdb.RecordStoreMetaDataStore;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocolTest;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.testing.TestingConnectorContext;
import io.prestosql.testing.TestingConnectorSession;
import org.junit.jupiter.api.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.List;
import java.util.function.Function;

// inspired from https://github.com/prestosql/presto/blob/master/presto-cassandra/src/test/java/io/prestosql/plugin/cassandra/TestCassandraConnector.java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PrestoConnectorTest {
  private final FoundationDBContainer container = new FoundationDBContainer();
  private File clusterFile;

  private ConnectorMetadata metadata;
  private static final ConnectorSession SESSION  = TestingConnectorSession.builder()
    .build();
  protected String database;


  @BeforeAll
    void setUp() {
      container.start();
      clusterFile = container.getClusterFile();

      RecordStoreConnectorFactory connectorFactory = new RecordStoreConnectorFactory();
      Connector connector = connectorFactory.create("test", ImmutableMap.of(
       "record-store.cluster-file", clusterFile.getAbsolutePath()
      ), new TestingConnectorContext());

      initContainer(clusterFile, "my-tenant", "my-container");

      metadata = connector.getMetadata(RecordStoreTransactionHandle.INSTANCE);
    }

  private void initContainer(File clusterFile, String tenant, String container) {
    FDBDatabase db = FDBDatabaseFactory.instance().getDatabase(clusterFile.getAbsolutePath());
    createSchema(db, tenant, container);
    // addData(db, tenant, container);
  }

  private void addData(FDBDatabase db, String tenantID, String container) {

    RecordStoreProtocolTest.Person person = RecordStoreProtocolTest.Person.newBuilder()
      .setId(1)
      .setName("Pierre")
      .setEmail("contact@test.org")
      .build();

    try (FDBRecordContext context = db.openContext()) {

      // create recordStoreProvider
      FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

      // Helper func
      Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context2 -> FDBRecordStore.newBuilder()
        .setMetaDataProvider(metaDataStore)
        .setContext(context)
        .setKeySpacePath(RecordStoreKeySpace.getDataKeySpacePath(tenantID, container))
        .createOrOpen();

      FDBRecordStore r = recordStoreProvider.apply(context);
      System.out.println(r.getRecordMetaData().getRecordsDescriptor());
      r.saveRecord(person);
      context.commit();
    }
    System.out.println("data added");
  }

  private void createSchema(FDBDatabase db, String tenantID, String container) {


    try (FDBRecordContext context = db.openContext()) {
      FDBMetaDataStore metaDataStore = RecordStoreMetaDataStore.createMetadataStore(context, tenantID, container);

      RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder();
        metadataBuilder.setRecords(RecordStoreProtocolTest.getDescriptor());
        metadataBuilder.getRecordType("Person").setPrimaryKey(Key.Expressions.field("id"));

      // set options
      metadataBuilder.setVersion(1);
      metadataBuilder.setStoreRecordVersions(true);
      metadataBuilder.setSplitLongRecords(true);

      metaDataStore.saveRecordMetaData(metadataBuilder.build().getRecordMetaData().toProto());

      context.commit();
    }
    System.out.println("metadata created");
  }

  @AfterEach
    void tearDown() {
    container.close();
    }

  @Test
  public void testGetDatabaseNames() {
    List<String> databases = metadata.listSchemaNames(SESSION);
  }
}
