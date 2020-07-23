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

import com.google.protobuf.DescriptorProtos;
import fr.pierrezemb.recordstore.GrpcVerticle;
import fr.pierrezemb.recordstore.PortManager;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.proto.DemoUserProto;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.protobuf.ProtobufReflectionUtil;
import io.grpc.ManagedChannel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

import java.io.File;
import java.util.Collections;

import static fr.pierrezemb.recordstore.GrpcVerticleTest.DEFAULT_CONTAINER;
import static fr.pierrezemb.recordstore.GrpcVerticleTest.DEFAULT_TENANT;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SchemaAdminServiceTest extends AbstractFDBContainer {

  public final int port = PortManager.nextFreePort();
  private SchemaServiceGrpc.SchemaServiceVertxStub schemaServiceVertxStub;
  private AdminServiceGrpc.AdminServiceVertxStub adminServiceVertxStub;
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

    schemaServiceVertxStub = SchemaServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
    adminServiceVertxStub = AdminServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
  }

  @RepeatedTest(value = 3)
  public void testCRUDSchema1(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoUserProto.User.getDescriptor());


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addRecordTypeIndexDefinitions(RecordStoreProtocol.RecordTypeIndexDefinition.newBuilder()
        .setName("User")
        .addPrimaryKeyFields("id")
        .build())
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema2(Vertx vertx, VertxTestContext testContext) throws Exception {
    schemaServiceVertxStub.get(RecordStoreProtocol.GetSchemaRequest.newBuilder()
      .setRecordTypeName("User")
      .build(), response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema3(Vertx vertx, VertxTestContext testContext) throws Exception {
    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoUserProto.User.getDescriptor());


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addRecordTypeIndexDefinitions(RecordStoreProtocol.RecordTypeIndexDefinition.newBuilder()
        .setName("User")
        .addPrimaryKeyFields("id")
        .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
          .setField("name")
          .setIndexType(RecordStoreProtocol.IndexType.VALUE)
          .build())
        .build())
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema4(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoUserProto.User.getDescriptor());

    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addRecordTypeIndexDefinitions(RecordStoreProtocol.RecordTypeIndexDefinition.newBuilder()
        .setName("User")
        .addPrimaryKeyFields("id")
        // let's forget an index, this is working as we cannot delete an Index for now
        .build())
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(new Throwable("should have failed"));
      }
    });
  }

  @RepeatedTest(value = 3)
  public void testCRUDSchema5(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoUserProto.User.getDescriptor());

    // upsert old schema should be harmless
    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .addRecordTypeIndexDefinitions(RecordStoreProtocol.RecordTypeIndexDefinition.newBuilder()
        .setName("User")
        .addPrimaryKeyFields("id")
        .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
          .setField("name").build())
        .build())
      .setSchema(dependencies)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema6(Vertx vertx, VertxTestContext testContext) throws Exception {
    adminServiceVertxStub.list(RecordStoreProtocol.ListContainerRequest.newBuilder().build(), response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testCRUDSchema7(Vertx vertx, VertxTestContext testContext) throws Exception {
    adminServiceVertxStub.delete(RecordStoreProtocol.DeleteContainerRequest.newBuilder()
      .addContainers(DEFAULT_CONTAINER)
      .build(), response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @AfterAll
  public void afterAll(Vertx vertx, VertxTestContext testContext) throws Exception {
    vertx.close();
    testContext.completeNow();
  }
}
