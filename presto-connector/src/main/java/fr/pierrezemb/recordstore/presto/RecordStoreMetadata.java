package fr.pierrezemb.recordstore.presto;

import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.List;

public class RecordStoreMetadata implements ConnectorMetadata {
  @Override
  public boolean schemaExists(ConnectorSession session, String schemaName) {
    return false;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return null;
  }
}
