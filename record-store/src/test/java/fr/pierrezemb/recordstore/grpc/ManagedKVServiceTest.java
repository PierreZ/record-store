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
package fr.pierrezemb.recordstore.grpc;

import com.google.protobuf.ByteString;
import fr.pierrezemb.recordstore.GrpcVerticle;
import fr.pierrezemb.recordstore.PortManager;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.proto.managed.kv.ManagedKVGrpc;
import fr.pierrezemb.recordstore.proto.managed.kv.ManagedKVProto;
import io.grpc.ManagedChannel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collections;

import static fr.pierrezemb.recordstore.GrpcVerticleTest.DEFAULT_TENANT;
import static org.junit.Assert.assertEquals;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ManagedKVServiceTest extends AbstractFDBContainer {

  public final int port = PortManager.nextFreePort();
  private ManagedKVGrpc.ManagedKVVertxStub managedKVVertxStub;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {

    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath())
        .put("grpc-listen-port", port));


    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());
    BiscuitClientCredential credentials = new BiscuitClientCredential(DEFAULT_TENANT, sealedBiscuit, this.getClass().getName());

    // deploy verticle
    vertx.deployVerticle(new GrpcVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx, "localhost", port)
      .usePlaintext(true)
      .build();

    managedKVVertxStub = ManagedKVGrpc.newVertxStub(channel).withCallCredentials(credentials);
  }

  @Test
  @Order(1)
  public void testPut(Vertx vertx, VertxTestContext testContext) throws Exception {

    managedKVVertxStub.put(ManagedKVProto.KeyValue.newBuilder()
      .setKey(ByteString.copyFrom("b", Charset.defaultCharset()))
      .setValue(ByteString.copyFrom("toto", Charset.defaultCharset()))
      .build(), response -> {
      if (response.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  @Order(2)
  public void testGet(Vertx vertx, VertxTestContext testContext) throws Exception {
    managedKVVertxStub.scan(ManagedKVProto.ScanRequest.newBuilder()
      .setStartKey(ByteString.copyFrom("b", Charset.defaultCharset()))
      .build(), keyValueGrpcReadStream -> keyValueGrpcReadStream.handler(keyValue -> {
      assertEquals(1, keyValue.getKey().size());
      assertEquals(4, keyValue.getValue().size());
      testContext.completeNow();
    }));
  }

  @Test
  @Order(3)
  public void testScan(Vertx vertx, VertxTestContext testContext) throws Exception {
    managedKVVertxStub.scan(ManagedKVProto.ScanRequest.newBuilder()
      .setStartKey(ByteString.copyFrom("a", Charset.defaultCharset()))
      .setEndKey(ByteString.copyFrom("c", Charset.defaultCharset()))
      .build(), keyValueGrpcReadStream -> keyValueGrpcReadStream.handler(keyValue -> {
      assertEquals(1, keyValue.getKey().size());
      assertEquals(4, keyValue.getValue().size());
      testContext.completeNow();
    }));
  }

  @Test
  @Order(4)
  public void testDelete(Vertx vertx, VertxTestContext testContext) throws Exception {
    managedKVVertxStub.delete(ManagedKVProto.DeleteRequest.newBuilder()
      .setKeyToDelete(ByteString.copyFrom("b", Charset.defaultCharset()))
      .build(), response -> {
      if (response.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }
}
