package fr.pierrezemb.recordstore.graphql;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.RecordQuery;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.utils.graphql.ProtoToGql;
import fr.pierrezemb.recordstore.utils.graphql.SchemaOptions;
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.json.JsonCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.VertxDataFetcher;
import io.vertx.ext.web.handler.graphql.impl.GraphQLBatch;
import io.vertx.ext.web.handler.graphql.impl.GraphQLInput;
import io.vertx.ext.web.handler.graphql.impl.GraphQLQuery;
import org.dataloader.DataLoaderRegistry;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static fr.pierrezemb.recordstore.datasets.DatasetsLoader.DEFAULT_DEMO_TENANT;
import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;
import static java.util.stream.Collectors.toList;

/**
 * Taken from https://github.com/vert-x3/vertx-web/blob/3.9/vertx-web-graphql/src/main/java/io/vertx/ext/web/handler/graphql/impl/GraphQLHandlerImpl.java
 */
public class RecordStoreGraphQLHandler implements GraphQLHandler {

  private static final Function<RoutingContext, Object> DEFAULT_QUERY_CONTEXT_FACTORY = rc -> rc;
  private static final Function<RoutingContext, DataLoaderRegistry> DEFAULT_DATA_LOADER_REGISTRY_FACTORY = rc -> null;
  private static final Function<RoutingContext, Locale> DEFAULT_LOCALE_FACTORY = rc -> null;
  private final RecordLayer recordLayer;

  private Function<RoutingContext, Object> queryContextFactory = DEFAULT_QUERY_CONTEXT_FACTORY;
  private Function<RoutingContext, DataLoaderRegistry> dataLoaderRegistryFactory = DEFAULT_DATA_LOADER_REGISTRY_FACTORY;
  private Function<RoutingContext, Locale> localeFactory = DEFAULT_LOCALE_FACTORY;

  public RecordStoreGraphQLHandler(RecordLayer recordLayer) {
    this.recordLayer = recordLayer;
  }

  @Override
  public synchronized GraphQLHandler queryContext(Function<RoutingContext, Object> factory) {
    queryContextFactory = factory != null ? factory : DEFAULT_QUERY_CONTEXT_FACTORY;
    return this;
  }

  @Override
  public synchronized GraphQLHandler dataLoaderRegistry(Function<RoutingContext, DataLoaderRegistry> factory) {
    dataLoaderRegistryFactory = factory != null ? factory : DEFAULT_DATA_LOADER_REGISTRY_FACTORY;
    return this;
  }

  @Override
  public synchronized GraphQLHandler locale(Function<RoutingContext, Locale> factory) {
    localeFactory = factory != null ? factory : DEFAULT_LOCALE_FACTORY;
    return this;
  }

  @Override
  public void handle(RoutingContext rc) {
    HttpMethod method = rc.request().method();
    if (method == GET) {
      handleGet(rc);
    } else if (method == POST) {
      Buffer body = rc.getBody();
      if (body == null) {
        rc.request().bodyHandler(buffer -> handlePost(rc, buffer));
      } else {
        handlePost(rc, body);
      }
    } else {
      rc.fail(405);
    }
  }

  private void handleGet(RoutingContext rc) {
    String query = rc.queryParams().get("query");
    if (query == null) {
      failQueryMissing(rc);
      return;
    }
    Map<String, Object> variables;
    try {
      variables = getVariablesFromQueryParam(rc);
    } catch (Exception e) {
      rc.fail(400, e);
      return;
    }
    executeOne(rc, new GraphQLQuery(query, rc.queryParams().get("operationName"), variables));
  }

  private void handlePost(RoutingContext rc, Buffer body) {
    Map<String, Object> variables;
    try {
      variables = getVariablesFromQueryParam(rc);
    } catch (Exception e) {
      rc.fail(400, e);
      return;
    }

    String query = rc.queryParams().get("query");
    if (query != null) {
      executeOne(rc, new GraphQLQuery(query, rc.queryParams().get("operationName"), variables));
      return;
    }

    switch (getContentType(rc)) {
      case "application/json":
        handlePostJson(rc, body, rc.queryParams().get("operationName"), variables);
        break;
      case "application/graphql":
        executeOne(rc, new GraphQLQuery(body.toString(), rc.queryParams().get("operationName"), variables));
        break;
      default:
        rc.fail(415);
    }
  }

  private void handlePostJson(RoutingContext rc, Buffer body, String operationName, Map<String, Object> variables) {
    GraphQLInput graphQLInput;
    try {
      graphQLInput = JsonCodec.INSTANCE.fromBuffer(body, GraphQLInput.class);
    } catch (Exception e) {
      rc.fail(400, e);
      return;
    }
    if (graphQLInput instanceof GraphQLBatch) {
      handlePostBatch(rc, (GraphQLBatch) graphQLInput, operationName, variables);
    } else if (graphQLInput instanceof GraphQLQuery) {
      handlePostQuery(rc, (GraphQLQuery) graphQLInput, operationName, variables);
    } else {
      rc.fail(500);
    }
  }

  private void handlePostBatch(RoutingContext rc, GraphQLBatch batch, String operationName, Map<String, Object> variables) {
    /**if (!options.isRequestBatchingEnabled()) {
     rc.fail(400);
     return;
     }*/
    for (GraphQLQuery query : batch) {
      if (query.getQuery() == null) {
        failQueryMissing(rc);
        return;
      }
      if (operationName != null) {
        query.setOperationName(operationName);
      }
      if (variables != null) {
        query.setVariables(variables);
      }
    }
    executeBatch(rc, batch);
  }

  private void executeBatch(RoutingContext rc, GraphQLBatch batch) {
    List<CompletableFuture<JsonObject>> results = batch.stream()
      .map(q -> execute(rc, q))
      .collect(toList());
    CompletableFuture.allOf(results.toArray(new CompletableFuture<?>[0]))
      .thenApply(v -> {
        JsonArray jsonArray = results.stream()
          .map(CompletableFuture::join)
          .collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
        return jsonArray.toBuffer();
      })
      .whenComplete((buffer, throwable) -> sendResponse(rc, buffer, throwable));
  }

  private void handlePostQuery(RoutingContext rc, GraphQLQuery query, String operationName, Map<String, Object> variables) {
    if (query.getQuery() == null) {
      failQueryMissing(rc);
      return;
    }
    if (operationName != null) {
      query.setOperationName(operationName);
    }
    if (variables != null) {
      query.setVariables(variables);
    }
    executeOne(rc, query);
  }

  private void executeOne(RoutingContext rc, GraphQLQuery query) {
    execute(rc, query)
      .thenApply(JsonObject::toBuffer)
      .whenComplete((buffer, throwable) -> sendResponse(rc, buffer, throwable));
  }

  private CompletableFuture<JsonObject> execute(RoutingContext rc, GraphQLQuery query) {
    ExecutionInput.Builder builder = ExecutionInput.newExecutionInput();

    builder.query(query.getQuery());
    String operationName = query.getOperationName();
    if (operationName != null) {
      builder.operationName(operationName);
    }
    Map<String, Object> variables = query.getVariables();
    if (variables != null) {
      builder.variables(variables);
    }

    Function<RoutingContext, Object> qc;
    synchronized (this) {
      qc = queryContextFactory;
    }
    builder.context(qc.apply(rc));

    Function<RoutingContext, DataLoaderRegistry> dlr;
    synchronized (this) {
      dlr = dataLoaderRegistryFactory;
    }
    DataLoaderRegistry registry = dlr.apply(rc);
    if (registry != null) {
      builder.dataLoaderRegistry(registry);
    }

    Function<RoutingContext, Locale> l;
    synchronized (this) {
      l = localeFactory;
    }
    Locale locale = l.apply(rc);
    if (locale != null) {
      builder.locale(locale);
    }

    GraphQL graphQL = createGraphQL(DEFAULT_DEMO_TENANT, "PERSONS");

    return graphQL.executeAsync(builder.build()).thenApplyAsync(executionResult -> {
      return new JsonObject(executionResult.toSpecification());
    }, contextExecutor(rc));
  }


  private String getContentType(RoutingContext rc) {
    String contentType = rc.parsedHeaders().contentType().value();
    return contentType.isEmpty() ? "application/json" : contentType.toLowerCase();
  }

  private Map<String, Object> getVariablesFromQueryParam(RoutingContext rc) throws Exception {
    String variablesParam = rc.queryParams().get("variables");
    if (variablesParam == null) {
      return null;
    } else {
      return new JsonObject(variablesParam).getMap();
    }
  }

  private void sendResponse(RoutingContext rc, Buffer buffer, Throwable throwable) {
    if (throwable == null) {
      rc.response().putHeader(HttpHeaders.CONTENT_TYPE, "application/json").end(buffer);
    } else {
      rc.fail(throwable);
    }
  }

  private void failQueryMissing(RoutingContext rc) {
    rc.fail(400, new NoStackTraceThrowable("Query is missing"));
  }

  private Executor contextExecutor(RoutingContext rc) {
    Context ctx = rc.vertx().getOrCreateContext();
    return command -> ctx.runOnContext(v -> command.run());
  }

  private GraphQL createGraphQL(String tenant, String container) {

    String schema = "";
    RecordMetaData metadata;
    try {
      metadata = this.recordLayer.getSchema(tenant, container);
      SchemaPrinter schemaPrinter = new SchemaPrinter();
      schema = metadata.getRecordTypes().values().stream()
        .map(e -> ProtoToGql.convert(e.getDescriptor(), SchemaOptions.defaultOptions()))
        .map(schemaPrinter::print)
        .collect(Collectors.joining("\n"));
    } catch (RuntimeException e) {
      return null;
    }

    schema += "\n type Query {" +
      " allPersons: [Person]" +
      "}";

    SchemaParser schemaParser = new SchemaParser();
    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

    RuntimeWiring runtimeWiring = newRuntimeWiring()
      .type("Query", builder -> {
        VertxDataFetcher<List<Map<String, Object>>> getAllRecords = new VertxDataFetcher<>(this::getAllRecords);
        return builder.dataFetcher("allPersons", getAllRecords);
      })
      .build();

    SchemaGenerator schemaGenerator = new SchemaGenerator();
    GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);

    return GraphQL.newGraphQL(graphQLSchema)
      .build();
  }

  private void getAllRecords(DataFetchingEnvironment env, Promise<List<Map<String, Object>>> future) {

    RecordQuery query = RecordQuery.newBuilder()
      .setRecordType("Person")
      .build();
    this.recordLayer.queryRecordsWithPromise("demo", "PERSONS", query, future);
  }
}
