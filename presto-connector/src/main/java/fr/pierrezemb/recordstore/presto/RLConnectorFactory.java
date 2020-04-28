package fr.pierrezemb.recordstore.presto;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import java.util.Map;

public class RLConnectorFactory implements ConnectorFactory {
  @Override
  public String getName() {
    return "record-store";
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new RLHandleResolver();
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
    return null;
  }
}
