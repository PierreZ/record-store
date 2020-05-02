package fr.pierrezemb.recordstore;


import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
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

import java.util.concurrent.TimeUnit;

import static fr.pierrezemb.recordstore.auth.BiscuitManager.DEFAULT_BISCUIT_KEY;

public class MainVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    String clusterFilePath = this.context.config().getString("fdb-cluster-file", "/var/fdb/fdb.cluster");
    System.out.println("connecting to fdb@" + clusterFilePath);

    String key = this.context.config().getString("biscuit-key", DEFAULT_BISCUIT_KEY);
    if (key.equals(DEFAULT_BISCUIT_KEY)) {
      LOGGER.warn("using default key for tokens");
    }

    FDBDatabase db = FDBDatabaseFactory.instance().getDatabase(clusterFilePath);
    try {
      db.performNoOpAsync().get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      System.err.println("could not perform a noop on fdb: " + e.getMessage());
      startPromise.fail(e);
      return;
    }
    System.out.println("connected to FDB!");
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
        System.out.println("gRPC service started on " + this.context.config().getInteger("listen-port"));
        startPromise.complete();
      } else {
        System.out.println("Could not start server " + ar.cause().getMessage());
        startPromise.fail(ar.cause());
      }
    });
  }
}
