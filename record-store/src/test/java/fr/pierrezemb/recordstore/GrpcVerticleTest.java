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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.proto.DemoUserProto;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.AbstractFDBContainer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GrpcVerticleTest extends AbstractFDBContainer {

  public static final String DEFAULT_TENANT = "my-tenant";
  public static final String DEFAULT_CONTAINER = "my-recordSpace";
  public final int port = PortManager.nextFreePort();
  private SchemaServiceGrpc.SchemaServiceVertxStub schemaServiceVertxStub;
  private RecordServiceGrpc.RecordServiceVertxStub recordServiceVertxStub;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) {

    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put(Constants.CONFIG_FDB_CLUSTER_FILE, clusterFile.getAbsolutePath())
        .put(Constants.CONFIG_GRPC_LISTEN_PORT, port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());
    System.out.println(sealedBiscuit);
    BiscuitClientCredential credentials = new BiscuitClientCredential(DEFAULT_TENANT, sealedBiscuit, this.getClass().getName());

    // deploy verticle
    vertx.deployVerticle(new GrpcVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx, "localhost", port)
      .usePlaintext(true)
      .build();

    schemaServiceVertxStub = SchemaServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
    recordServiceVertxStub = RecordServiceGrpc.newVertxStub(channel).withCallCredentials(credentials);
  }

  @Test
  public void testCreateSchema(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoUserProto.User.getDescriptor());

    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setSchema(dependencies)
      .addRecordTypeIndexDefinitions(
        RecordStoreProtocol.RecordTypeIndexDefinition.newBuilder()
          .setName("User")
          .addPrimaryKeyFields("id")
          .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
            .setIndexType(RecordStoreProtocol.IndexType.VERSION)
            .build())
          .build()
      )
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
  public void testPut1(Vertx vertx, VertxTestContext testContext) throws Exception {

    DemoUserProto.User person = DemoUserProto.User.newBuilder()
      .setId(1)
      .setName("PierreZ")
      .setEmail("toto@example.com")
      .build();

    RecordStoreProtocol.PutRecordRequest request = RecordStoreProtocol.PutRecordRequest.newBuilder()
      .setRecordTypeName("User")
      .setMessage(person.toByteString())
      .build();

    recordServiceVertxStub.put(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testPut2(Vertx vertx, VertxTestContext testContext) throws Exception {
    RecordStoreProtocol.StatRequest recordRequest = RecordStoreProtocol.StatRequest.newBuilder()
      .build();

    schemaServiceVertxStub.stat(recordRequest, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        System.out.println("there is " + response.result().getCount() + " records");
        System.out.println("there is " + response.result().getCountUpdates() + " updates");
        assertEquals(1, response.result().getCount());
        assertEquals(1, response.result().getCountUpdates());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testPut3(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordStoreProtocol.QueryFilterNode query = RecordStoreProtocol.QueryFilterNode.newBuilder()
      .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
        .setField("id")
        .setInt64Value(2)
        .setOperation(RecordStoreProtocol.FilterOperation.LESS_THAN_OR_EQUALS)
        .build())
      .build();

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setRecordTypeName("User")
      .setFilter(query)
      .build();

    recordServiceVertxStub.query(request, response -> {
      response.handler(req -> {
        System.out.println("received a response");
        DemoUserProto.User p = null;
        try {
          p = DemoUserProto.User.parseFrom(req.getRecord());
          assertEquals("PierreZ", p.getName());
          assertEquals("toto@example.com", p.getEmail());
          assertEquals(1, p.getId());
        } catch (InvalidProtocolBufferException e) {
          testContext.failNow(e);
          e.printStackTrace();
        }
      });
      response.endHandler(end -> testContext.completeNow());
      response.exceptionHandler(testContext::failNow);
    });
  }

  @Test
  public void testPut4(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordStoreProtocol.QueryFilterAndNode andNode = RecordStoreProtocol.QueryFilterAndNode.newBuilder()
      .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
        .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField("id")
          .setInt64Value(2)
          .setOperation(RecordStoreProtocol.FilterOperation.LESS_THAN_OR_EQUALS)
          .build()).build())
      .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
        .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField("id")
          .setInt64Value(1)
          .setOperation(RecordStoreProtocol.FilterOperation.GREATER_THAN_OR_EQUALS)
          .build()).build())
      .build();

    RecordStoreProtocol.QueryFilterNode query = RecordStoreProtocol.QueryFilterNode.newBuilder()
      .setAndNode(andNode)
      .build();

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setRecordTypeName("User")
      .setFilter(query)
      .build();

    recordServiceVertxStub.query(request, response -> {
      response.handler(req -> {
        System.out.println("received a response");
        DemoUserProto.User p = null;
        try {
          p = DemoUserProto.User.parseFrom(req.getRecord());
          assertEquals("PierreZ", p.getName());
          assertEquals("toto@example.com", p.getEmail());
          assertEquals(1, p.getId());
        } catch (InvalidProtocolBufferException e) {
          testContext.failNow(e);
          e.printStackTrace();
        }
      });
      response.endHandler(end -> testContext.completeNow());
      response.exceptionHandler(testContext::failNow);
    });
  }

  @Test
  public void testPut5(Vertx vertx, VertxTestContext testContext) throws Exception {
    RecordStoreProtocol.QueryFilterAndNode andNode = RecordStoreProtocol.QueryFilterAndNode.newBuilder()
      .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
        .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField("id")
          .setInt64Value(2)
          .setOperation(RecordStoreProtocol.FilterOperation.LESS_THAN_OR_EQUALS)
          .build()).build())
      .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
        .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField("id")
          .setInt64Value(1)
          .setOperation(RecordStoreProtocol.FilterOperation.GREATER_THAN_OR_EQUALS)
          .build()).build())
      .build();

    RecordStoreProtocol.QueryFilterNode query = RecordStoreProtocol.QueryFilterNode.newBuilder()
      .setAndNode(andNode)
      .build();

    RecordStoreProtocol.DeleteRecordRequest request = RecordStoreProtocol.DeleteRecordRequest.newBuilder()
      .setFilter(query)
      .setRecordTypeName("User")
      .build();

    recordServiceVertxStub.delete(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result());
        assertEquals(1, response.result().getDeletedCount());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testPut6(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordStoreProtocol.QueryFilterAndNode andNode = RecordStoreProtocol.QueryFilterAndNode.newBuilder()
      .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
        .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField("id")
          .setInt64Value(2)
          .setOperation(RecordStoreProtocol.FilterOperation.LESS_THAN_OR_EQUALS)
          .build()).build())
      .addNodes(RecordStoreProtocol.QueryFilterNode.newBuilder()
        .setFieldNode(RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField("id")
          .setInt64Value(1)
          .setOperation(RecordStoreProtocol.FilterOperation.GREATER_THAN_OR_EQUALS)
          .build()).build())
      .build();

    RecordStoreProtocol.QueryFilterNode query = RecordStoreProtocol.QueryFilterNode.newBuilder()
      .setAndNode(andNode)
      .build();

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setRecordTypeName("User")
      .setFilter(query)
      .build();

    recordServiceVertxStub.query(request, response -> {
      List<DemoUserProto.User> results = new ArrayList<>();
      response.handler(req -> {
        System.out.println("received a response");
        DemoUserProto.User p = null;
        try {
          p = DemoUserProto.User.parseFrom(req.getRecord());
          results.add(p);
        } catch (InvalidProtocolBufferException e) {
          testContext.failNow(e);
          e.printStackTrace();
        }
      });
      response.endHandler(end -> {
        assertEquals(0, results.size());
        testContext.completeNow();
      });
      response.exceptionHandler(testContext::failNow);
    });
  }

  @Test
  public void testPut7(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setRecordTypeName("User")
      .setSortBy(RecordStoreProtocol.SortByRequest.newBuilder().setType(RecordStoreProtocol.SortByType.SORT_BY_NEWEST_VERSION_FIRST)
        .build())
      .build();

    recordServiceVertxStub.query(request, response -> {
      List<DemoUserProto.User> results = new ArrayList<>();
      response.handler(req -> {
        System.out.println("received a response");
        DemoUserProto.User p = null;
        try {
          p = DemoUserProto.User.parseFrom(req.getRecord());
          results.add(p);
        } catch (InvalidProtocolBufferException e) {
          testContext.failNow(e);
          e.printStackTrace();
        }
      });
      response.endHandler(end -> {
        assertEquals(0, results.size());
        testContext.completeNow();
      });
      response.exceptionHandler(testContext::failNow);
    });
  }

  @AfterAll
  public void afterAll(Vertx vertx, VertxTestContext testContext) throws Exception {
    vertx.close();
    testContext.completeNow();
  }
}
