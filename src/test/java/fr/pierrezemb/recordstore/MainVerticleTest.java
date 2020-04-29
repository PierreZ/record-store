package fr.pierrezemb.recordstore;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.proto.RecordServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocolTest;
import fr.pierrezemb.recordstore.proto.SchemaServiceGrpc;
import fr.pierrezemb.recordstore.utils.ProtobufReflectionUtil;
import io.grpc.ManagedChannel;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.grpc.VertxChannelBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MainVerticleTest {

  private final FoundationDBContainer container = new FoundationDBContainer();
  private SchemaServiceGrpc.SchemaServiceVertxStub schemaServiceVertxStub;
  private RecordServiceGrpc.RecordServiceVertxStub recordServiceVertxStub;
  private File clusterFile;

  @BeforeAll
  void deploy_verticle(Vertx vertx, VertxTestContext testContext) throws IOException, InterruptedException {

    container.start();
    clusterFile = container.getClusterFile();

    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put("fdb-cluster-file", clusterFile.getAbsolutePath())
      );

    // deploy verticle
    vertx.deployVerticle(new MainVerticle(), options, testContext.succeeding(id -> testContext.completeNow()));
    ManagedChannel channel = VertxChannelBuilder
      .forAddress(vertx, "localhost", 8080)
      .usePlaintext(true)
      .build();

    schemaServiceVertxStub = SchemaServiceGrpc.newVertxStub(channel);
    recordServiceVertxStub = RecordServiceGrpc.newVertxStub(channel);
  }

  @Test
  public void testCreateSchema(Vertx vertx, VertxTestContext testContext) throws Exception {

    DescriptorProtos.FileDescriptorSet dependencies =
      ProtobufReflectionUtil.protoFileDescriptorSet(RecordStoreProtocolTest.Person.getDescriptor());

    RecordStoreProtocol.SelfDescribedMessage selfDescribedMessage = RecordStoreProtocol.SelfDescribedMessage
      .newBuilder()
      .setDescriptorSet(dependencies)
      .build();


    RecordStoreProtocol.UpsertSchemaRequest request = RecordStoreProtocol.UpsertSchemaRequest
      .newBuilder()
      .setName("Person")
      .setPrimaryKeyField("id")
      .setSchema(selfDescribedMessage)
      .build();

    schemaServiceVertxStub.upsert(request, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result().getResult());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testPut1(Vertx vertx, VertxTestContext testContext) throws Exception {

    RecordStoreProtocolTest.Person person = RecordStoreProtocolTest.Person.newBuilder()
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
        System.out.println("Got the server response: " + response.result().getResult());
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }

  @Test
  public void testPut2(Vertx vertx, VertxTestContext testContext) throws Exception {
    RecordStoreProtocol.CountRecordRequest recordRequest = RecordStoreProtocol.CountRecordRequest.newBuilder()
      .setTable("Person")
      .build();

    recordServiceVertxStub.count(recordRequest, response -> {
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result().getResult());
        System.out.println("there is " + response.result().getSize() + " records");
        assertEquals(1, response.result().getSize());
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
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result().getResult());
        System.out.println(response.result().getRecordsList());
        assertEquals(1, response.result().getRecordsCount());
        try {
          RecordStoreProtocolTest.Person p = RecordStoreProtocolTest.Person.parseFrom(response.result().getRecords(0));
          assertEquals("PierreZ", p.getName());
          assertEquals("toto@example.com", p.getEmail());
          assertEquals(1, p.getId());
        } catch (InvalidProtocolBufferException e) {
          testContext.failNow(e);
        }
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
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
      if (response.succeeded()) {
        System.out.println("Got the server response: " + response.result().getResult());
        System.out.println(response.result().getRecordsList());
        assertEquals(1, response.result().getRecordsCount());
        try {
          RecordStoreProtocolTest.Person p = RecordStoreProtocolTest.Person.parseFrom(response.result().getRecords(0));
          assertEquals("PierreZ", p.getName());
          assertEquals("toto@example.com", p.getEmail());
          assertEquals(1, p.getId());
        } catch (InvalidProtocolBufferException e) {
          testContext.failNow(e);
        }
        testContext.completeNow();
      } else {
        testContext.failNow(response.cause());
      }
    });
  }
}
