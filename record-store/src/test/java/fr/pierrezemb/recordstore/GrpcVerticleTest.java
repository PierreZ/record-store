package fr.pierrezemb.recordstore;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.auth.BiscuitClientCredential;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import fr.pierrezemb.recordstore.datasets.proto.DemoPersonProto;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import io.grpc.ManagedChannel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GrpcVerticleTest {

  public static final String DEFAULT_TENANT = "my-tenant";
  public static final String DEFAULT_CONTAINER = "my-container";
  public final int port = PortManager.nextFreePort();
  private final FoundationDBContainer container = new FoundationDBContainer();
  private SchemaServiceGrpc.SchemaServiceVertxStub schemaServiceVertxStub;
  private RecordServiceGrpc.RecordServiceVertxStub recordServiceVertxStub;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject()
        .put("fdb-cluster-file", clusterFile.getAbsolutePath())
        .put("grpc-listen-port", port));

    BiscuitManager biscuitManager = new BiscuitManager();
    String sealedBiscuit = biscuitManager.create(DEFAULT_TENANT, Collections.emptyList());
    System.out.println(sealedBiscuit);
    BiscuitClientCredential credentials = new BiscuitClientCredential(DEFAULT_TENANT, sealedBiscuit, DEFAULT_CONTAINER);

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
      ProtobufReflectionUtil.protoFileDescriptorSet(DemoPersonProto.Person.getDescriptor());

    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setName("Person")
      .addPrimaryKeyFields("id")
      .setSchema(dependencies)
      // keep track of the versions
      .addIndexDefinitions(RecordStoreProtocol.IndexDefinition.newBuilder()
        .setIndexType(RecordStoreProtocol.IndexType.VERSION)
        .build())
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

    DemoPersonProto.Person person = DemoPersonProto.Person.newBuilder()
      .setId(1)
      .setName("PierreZ")
      .setEmail("toto@example.com")
      .build();

    RecordStoreProtocol.PutRecordRequest request = RecordStoreProtocol.PutRecordRequest.newBuilder()
      .setTable("Person")
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

    RecordStoreProtocol.Node query = RecordStoreProtocol.Node.newBuilder()
      .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
        .setField("id")
        .setInt64Value(2)
        .setOperation(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS)
        .build())
      .build();

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setTable("Person")
      .setQueryNode(query)
      .build();

    recordServiceVertxStub.query(request, response -> {
      response.handler(req -> {
        System.out.println("received a response");
        DemoPersonProto.Person p = null;
        try {
          p = DemoPersonProto.Person.parseFrom(req.getRecord());
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

    RecordStoreProtocol.AndNode andNode = RecordStoreProtocol.AndNode.newBuilder()
      .addNodes(RecordStoreProtocol.Node.newBuilder()
        .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
          .setField("id")
          .setInt64Value(2)
          .setOperation(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS)
          .build()).build())
      .addNodes(RecordStoreProtocol.Node.newBuilder()
        .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
          .setField("id")
          .setInt64Value(1)
          .setOperation(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS)
          .build()).build())
      .build();

    RecordStoreProtocol.Node query = RecordStoreProtocol.Node.newBuilder()
      .setAndNode(andNode)
      .build();

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setTable("Person")
      .setQueryNode(query)
      .build();

    recordServiceVertxStub.query(request, response -> {
      response.handler(req -> {
        System.out.println("received a response");
        DemoPersonProto.Person p = null;
        try {
          p = DemoPersonProto.Person.parseFrom(req.getRecord());
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
    RecordStoreProtocol.AndNode andNode = RecordStoreProtocol.AndNode.newBuilder()
      .addNodes(RecordStoreProtocol.Node.newBuilder()
        .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
          .setField("id")
          .setInt64Value(2)
          .setOperation(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS)
          .build()).build())
      .addNodes(RecordStoreProtocol.Node.newBuilder()
        .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
          .setField("id")
          .setInt64Value(1)
          .setOperation(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS)
          .build()).build())
      .build();

    RecordStoreProtocol.Node query = RecordStoreProtocol.Node.newBuilder()
      .setAndNode(andNode)
      .build();

    RecordStoreProtocol.DeleteRecordRequest request = RecordStoreProtocol.DeleteRecordRequest.newBuilder()
      .setQueryNode(query)
      .setTable("Person")
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

    RecordStoreProtocol.AndNode andNode = RecordStoreProtocol.AndNode.newBuilder()
      .addNodes(RecordStoreProtocol.Node.newBuilder()
        .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
          .setField("id")
          .setInt64Value(2)
          .setOperation(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS)
          .build()).build())
      .addNodes(RecordStoreProtocol.Node.newBuilder()
        .setFieldNode(RecordStoreProtocol.FieldNode.newBuilder()
          .setField("id")
          .setInt64Value(1)
          .setOperation(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS)
          .build()).build())
      .build();

    RecordStoreProtocol.Node query = RecordStoreProtocol.Node.newBuilder()
      .setAndNode(andNode)
      .build();

    RecordStoreProtocol.QueryRequest request = RecordStoreProtocol.QueryRequest.newBuilder()
      .setTable("Person")
      .setQueryNode(query)
      .build();

    recordServiceVertxStub.query(request, response -> {
      List<DemoPersonProto.Person> results = new ArrayList<>();
      response.handler(req -> {
        System.out.println("received a response");
        DemoPersonProto.Person p = null;
        try {
          p = DemoPersonProto.Person.parseFrom(req.getRecord());
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
      .setTable("Person")
      .setSortBy(RecordStoreProtocol.SortByRequest.newBuilder().setType(RecordStoreProtocol.SortByType.SORT_BY_NEWEST_VERSION_FIRST)
        .build())
      .build();

    recordServiceVertxStub.query(request, response -> {
      List<DemoPersonProto.Person> results = new ArrayList<>();
      response.handler(req -> {
        System.out.println("received a response");
        DemoPersonProto.Person p = null;
        try {
          p = DemoPersonProto.Person.parseFrom(req.getRecord());
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
}
