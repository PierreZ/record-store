package fr.pierrezemb.recordstore.presto;

import io.airlift.configuration.Config;

public class RecordStoreConnectorConfig implements AutoCloseable {
  private String clusterFile = "";

  @Config("record-store.cluster-file")
  public RecordStoreConnectorConfig setClusterFile(String clusterFile) {
    this.clusterFile = clusterFile;
    return this;
  }

  public String getClusterFile() {
    return clusterFile;
  }

  @Override
  public void close() throws Exception {

  }
}
