package fr.pierrezemb.recordstore;


import static fr.pierrezemb.recordstore.auth.BiscuitManager.DEFAULT_BISCUIT_KEY;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import fr.pierrezemb.recordstore.fdb.metrics.FDBMetricsStoreTimer;
import fr.pierrezemb.recordstore.grpc.AdminService;
import fr.pierrezemb.recordstore.grpc.AuthInterceptor;
import fr.pierrezemb.recordstore.grpc.RecordService;
import fr.pierrezemb.recordstore.grpc.SchemaService;
import io.micrometer.core.instrument.Metrics;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    String clusterFilePath = this.context.config().getString("fdb-cluster-file", "/var/fdb/fdb.cluster");
    System.out.println("connecting to fdb@" + clusterFilePath);

    // TODO: find a better options to set vertx options than to override everything
    Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
      new MicrometerMetricsOptions()
        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true)
          .setStartEmbeddedServer(true)
          .setEmbeddedServerOptions(new HttpServerOptions().setPort(8081))
          .setEmbeddedServerEndpoint("/metrics"))
        .setEnabled(true)));
    Metrics.addRegistry(BackendRegistries.getDefaultNow());

    String key = this.context.config().getString("biscuit-key", DEFAULT_BISCUIT_KEY);
    if (key.equals(DEFAULT_BISCUIT_KEY)) {
      LOGGER.warn("using default key for tokens");
    }

    FDBDatabase db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);
    FDBMetricsStoreTimer fdbStoreTimer = new FDBMetricsStoreTimer();

    VertxServerBuilder serverBuilder = VertxServerBuilder
      .forAddress(vertx,
        this.context.config().getString("listen-address", "localhost"),
        this.context.config().getInteger("listen-port", 8080))
      .intercept(new AuthInterceptor(key))
      .addService(new AdminService(db, fdbStoreTimer))
      .addService(new SchemaService(db, fdbStoreTimer))
      .addService(new RecordService(db, fdbStoreTimer));

    VertxServer server = serverBuilder.build();

    server.start(ar -> {
      if (ar.succeeded()) {
        System.out.println("gRPC service started");
        startPromise.complete();
      } else {
        System.out.println("Could not start server " + ar.cause().getMessage());
        startPromise.fail(ar.cause());
      }
    });
  }
}
