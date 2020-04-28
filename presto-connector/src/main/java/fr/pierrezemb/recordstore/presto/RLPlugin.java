package fr.pierrezemb.recordstore.presto;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

public class RLPlugin implements Plugin {
  @Override
  public Iterable<ConnectorFactory> getConnectorFactories()
  {
    return ImmutableList.of(new RLConnectorFactory());
  }
}
