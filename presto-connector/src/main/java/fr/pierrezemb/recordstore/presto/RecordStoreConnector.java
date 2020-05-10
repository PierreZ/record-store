package fr.pierrezemb.recordstore.presto;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.spi.connector.*;
import io.prestosql.spi.transaction.IsolationLevel;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static java.util.Objects.requireNonNull;

public class RecordStoreConnector implements Connector {
  private static final Logger log = Logger.get(RecordStoreConnector.class);

  private final LifeCycleManager lifeCycleManager;
  private final RecordStoreMetadata metadata;
  private final RecordStoreSplitManager splitManager;
  private final RecordStoreRecordSetProvider recordSetProvider;
  private final RecordStoreConnectorConfig recordStoreConnectorConfig;

  @Inject
  public RecordStoreConnector(
    LifeCycleManager lifeCycleManager,
    RecordStoreMetadata metadata,
    RecordStoreSplitManager splitManager,
    RecordStoreRecordSetProvider recordSetProvider,
    RecordStoreConnectorConfig recordStoreConnectorConfig
  ) {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    this.recordStoreConnectorConfig = requireNonNull(recordStoreConnectorConfig, "RecordStoreConnectorConfig is null");
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
    IsolationLevel.checkConnectorSupports(READ_COMMITTED, isolationLevel);
    return RecordStoreTransactionHandle.INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
    System.out.println(this.recordStoreConnectorConfig.getClusterFile());
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider() {
    return recordSetProvider;
  }

  @Override
  public final void shutdown() {
    try {
      this.recordStoreConnectorConfig.close();
    } catch (Exception e) {
      log.error(e, "Failed to close RecordStore connector");
    }
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      log.error(e, "Error shutting down connector");
    }
  }
}
