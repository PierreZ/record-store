/**
 * Copyright 2020 Pierre Zemb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.pierrezemb.recordstore;


import fr.pierrezemb.recordstore.datasets.DatasetsLoader;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.grpc.AdminService;
import fr.pierrezemb.recordstore.grpc.AuthInterceptor;
import fr.pierrezemb.recordstore.grpc.ManagedKVService;
import fr.pierrezemb.recordstore.grpc.RecordService;
import fr.pierrezemb.recordstore.grpc.SchemaService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.grpc.VertxServer;
import io.vertx.grpc.VertxServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;

import static fr.pierrezemb.recordstore.Constants.CONFIG_BISCUIT_KEY_DEFAULT;


public class GrpcVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcVerticle.class);

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    String clusterFilePath = this.context.config().getString(Constants.CONFIG_FDB_CLUSTER_FILE, Constants.CONFIG_FDB_CLUSTER_FILE_DEFAULT);
    LOGGER.info("connecting to fdb@" + clusterFilePath);

    boolean enableManagedKV = this.context.config().getBoolean(Constants.CONFIG_ENABLE_MANAGED_KV, Constants.CONFIG_ENABLE_MANAGED_KV_DEFAULT);

    String tokenKey = this.context.config().getString(Constants.CONFIG_BISCUIT_KEY, CONFIG_BISCUIT_KEY_DEFAULT);
    if (tokenKey.equals(CONFIG_BISCUIT_KEY_DEFAULT)) {
      LOGGER.warn("using default key for tokens");
    }

    byte[] key = this.context.config().getString(Constants.CONFIG_ENCRYPTION_KEY, Constants.CONFIG_ENCRYPTION_KEY_DEFAULT).getBytes();
    if (new String(key).equals(Constants.CONFIG_ENCRYPTION_KEY_DEFAULT)) {
      LOGGER.warn("using default encryption key for records");
    }
    SecretKeySpec secretKey = new SecretKeySpec(key, "AES");

    RecordLayer recordLayer = new RecordLayer(clusterFilePath, vertx.isMetricsEnabled(), secretKey);

    DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);
    datasetsLoader.loadDataset(this.context.config().getString(Constants.CONFIG_LOAD_DEMO, ""));

    VertxServerBuilder serverBuilder = VertxServerBuilder
      .forAddress(vertx,
        this.context.config().getString(Constants.CONFIG_GRPC_LISTEN_ADDRESS, "localhost"),
        this.context.config().getInteger(Constants.CONFIG_GRPC_LISTEN_PORT, 8080))
      .intercept(new AuthInterceptor(tokenKey))
      .addService(new AdminService(recordLayer))
      .addService(new SchemaService(recordLayer))
      .addService(new RecordService(recordLayer));

    if (enableManagedKV) {
      LOGGER.info("enabling ManagedKV");
      serverBuilder.addService(new ManagedKVService(recordLayer));
    }

    VertxServer server = serverBuilder.build();

    server.start(ar -> {
      if (ar.succeeded()) {
        System.out.println("gRPC service started on " + this.context.config().getInteger("grpc-listen-port"));
        startPromise.complete();
      } else {
        System.out.println("Could not start server " + ar.cause().getMessage());
        startPromise.fail(ar.cause());
      }
    });
  }
}
