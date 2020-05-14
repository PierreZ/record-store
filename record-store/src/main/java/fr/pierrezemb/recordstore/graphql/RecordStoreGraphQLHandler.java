package fr.pierrezemb.recordstore.graphql;

import fr.pierrezemb.recordstore.GraphQLVerticle;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import org.dataloader.DataLoaderRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.function.Function;

public class RecordStoreGraphQLHandler implements GraphQLHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordStoreGraphQLHandler.class);
  @Override
  public GraphQLHandler queryContext(Function<RoutingContext, Object> factory) {
    LOGGER.debug("received a factory on queryContext: {}", factory.toString());
    return null;
  }

  @Override
  public GraphQLHandler dataLoaderRegistry(Function<RoutingContext, DataLoaderRegistry> factory) {
    LOGGER.debug("received a factory on dataLoaderRegistry: {}", factory.toString());
    return null;
  }

  @Override
  public GraphQLHandler locale(Function<RoutingContext, Locale> factory) {
    LOGGER.debug("received a factory on locale: {}", factory.toString());
    return null;
  }

  @Override
  public void handle(RoutingContext event) {
    LOGGER.debug("received a event on handle: {}", event.toString());
  }
}
