package fr.pierrezemb.recordstore.presto;


import io.prestosql.spi.connector.ConnectorTransactionHandle;

/**
 * A handle for transactions.
 */
public enum RecordStoreTransactionHandle implements ConnectorTransactionHandle {
  INSTANCE
}

