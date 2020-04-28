package fr.pierrezemb.recordstore.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class RLHandleResolver implements ConnectorHandleResolver {
  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorIndexHandle> getIndexHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
    return null;
  }
}
