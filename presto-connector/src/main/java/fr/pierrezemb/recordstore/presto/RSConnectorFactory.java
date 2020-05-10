package fr.pierrezemb.recordstore.presto;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class RSConnectorFactory implements ConnectorFactory {
  @Override
  public String getName() {
    return "record-store";
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new RSHandleResolver();
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {

    requireNonNull(config, "requiredConfig is null");
    try {
      // A plugin is not required to use Guice; it is just very convenient
      Bootstrap app = new Bootstrap(
        new JsonModule(),
        new RSModule(catalogName, context.getTypeManager()));

      Injector injector = app
        .strictConfig()
        .doNotInitializeLogging()
        .setRequiredConfigurationProperties(config)
        .initialize();

      return injector.getInstance(RSConnector.class);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
