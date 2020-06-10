package fr.pierrezemb.recordstore.query;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Field;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GrpcQueryGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcQueryGenerator.class);

  public static RecordQuery generate(RecordStoreProtocol.QueryRequest request) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType(request.getRecordTypeName());

    if (request.getFieldsToReturnCount() > 0) {
      queryBuilder.setRequiredResults(request.getFieldsToReturnList().asByteStringList()
        .stream()
        .map(e -> Key.Expressions.field(String.valueOf(e.toString()))).collect(Collectors.toList()));
    }

    if (request.hasSortBy()) {
      System.out.println("sorting by" + request.getSortBy().getType());
      switch (request.getSortBy().getType()) {
        case SORT_DISABLE:
          break;
        case SORT_BY_OLDEST_VERSION_FIRST:
          queryBuilder.setSort(VersionKeyExpression.VERSION);
          break;
        case SORT_BY_NEWEST_VERSION_FIRST:
          queryBuilder.setSort(VersionKeyExpression.VERSION, true);
          break;
        case SORT_BY_VALUE:
          queryBuilder.setSort(Key.Expressions.field(request.getSortBy().getField()));
          break;
        case SORT_BY_VALUE_REVERSED:
          queryBuilder.setSort(Key.Expressions.field(request.getSortBy().getField()), true);
          break;
        case UNRECOGNIZED:
          throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("cannot recognize sortBy"));
      }
    }

    try {
      QueryComponent queryComponents = parseNode(request.getQueryNode());
      return queryBuilder.setFilter(queryComponents).build();
    } catch (ParseException e) {
      throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withCause(e));
    }
  }

  public static RecordQuery generate(RecordStoreProtocol.DeleteRecordRequest request) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType(request.getRecordTypeName());

    try {
      QueryComponent queryComponents = parseNode(request.getQueryNode());
      if (queryComponents != null) {
        queryBuilder.setFilter(queryComponents);
      }
    } catch (ParseException e) {
      return null;
    }
    return queryBuilder.build();
  }

  public static QueryComponent parseNode(RecordStoreProtocol.Node node) throws ParseException {

    if (node == null) {
      return null;
    }

    switch (node.getContentCase()) {

      case FIELD_NODE:
        return parseFieldNode(node.getFieldNode());
      case AND_NODE:
        return Query.and(parseChildrenNodes(node.getAndNode()));
      case OR_NODE:
        return Query.or(parseChildrenNodes(node.getOrNode()));
      case MAP_NODE:
        return handleMapNode(node.getMapNode());
    }
    return null;
  }

  private static QueryComponent handleMapNode(RecordStoreProtocol.MapNode mapNode) {

    if (mapNode.hasKey() && !mapNode.hasValue()) {
      return Query.field(mapNode.getField()).mapMatches(constructFunctionMatcher(mapNode.getKey()), null);
    }

    if (!mapNode.hasKey() && mapNode.hasValue()) {
      return Query.field(mapNode.getField()).mapMatches(null, constructFunctionMatcher(mapNode.getValue()));
    }

    return Query.field(mapNode.getField()).mapMatches(
      constructFunctionMatcher(mapNode.getKey()),
      constructFunctionMatcher(mapNode.getValue())
    );
  }

  private static Function<Field, QueryComponent> constructFunctionMatcher(RecordStoreProtocol.FieldNode node) {
    return k -> {
      try {
        return switchOnOperations(k, node);
      } catch (ParseException e) {
        LOGGER.error("cannot parse key query of map {}: {}", node, e);
        return null;
      }
    };
  }


  private static List<QueryComponent> parseChildrenNodes(RecordStoreProtocol.OrNode node) throws ParseException {
    List<QueryComponent> queryComponents = new ArrayList<>();
    for (RecordStoreProtocol.Node children : node.getNodesList()) {
      queryComponents.add(parseNode(children));
    }
    return queryComponents;
  }

  private static List<QueryComponent> parseChildrenNodes(RecordStoreProtocol.AndNode node) throws ParseException {
    List<QueryComponent> queryComponents = new ArrayList<>();
    for (RecordStoreProtocol.Node children : node.getNodesList()) {
      queryComponents.add(parseNode(children));
    }
    return queryComponents;
  }

  private static QueryComponent parseFieldNode(RecordStoreProtocol.FieldNode node) throws ParseException {
    if (node == null) {
      throw new ParseException("node is null", 0);
    }

    Field temporaryQuery = Query.field(node.getField());

    return switchOnOperations(temporaryQuery, node);
  }

  private static QueryComponent switchOnOperations(Field temporaryQuery, RecordStoreProtocol.FieldNode node) throws ParseException {
    switch (node.getOperation()) {
      case GREATER_THAN_OR_EQUALS:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().greaterThanOrEquals(parseValue(node)) :
          temporaryQuery.greaterThanOrEquals(parseValue(node));
      case LESS_THAN_OR_EQUALS:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().lessThanOrEquals(parseValue(node)) :
          temporaryQuery.lessThanOrEquals(parseValue(node));
      case GREATER_THAN:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().greaterThan(parseValue(node)) :
          temporaryQuery.greaterThan(parseValue(node));
      case LESS_THAN:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().lessThan(parseValue(node)) :
          temporaryQuery.lessThan(parseValue(node));
      case START_WITH:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().startsWith(String.valueOf(parseValue(node))) :
          temporaryQuery.startsWith(String.valueOf(parseValue(node)));
      case IS_EMPTY:
        return Query.field(node.getField()).isEmpty();
      case IS_NULL:
        return Query.field(node.getField()).isNull();
      case EQUALS:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().equalsValue(parseValue(node)) :
          temporaryQuery.equalsValue(parseValue(node));
      case NOT_EQUALS:
        return node.getIsFieldDefinedAsRepeated() ?
          temporaryQuery.oneOfThem().notEquals(parseValue(node)) :
          temporaryQuery.notEquals(parseValue(node));
      case NOT_NULL:
        return Query.field(node.getField()).notNull();
      case MATCHES:
        if (node.getValueCase() != RecordStoreProtocol.FieldNode.ValueCase.FIELDNODE) {
          throw new ParseException("Matches onl accept a nested FieldValue", 0);
        }
        return Query.field(node.getField()).matches(Objects.requireNonNull(parseFieldNode(node.getFieldNode())));
      case TEXT_CONTAINS_ANY:
        return Query.field(node.getField()).text().containsAny(node.getTokensList());
      case TEXT_CONTAINS_ALL:
        return Query.field(node.getField()).text().containsAll(node.getTokensList());
      case UNRECOGNIZED:
        throw new ParseException("unrecognized field on node " + node.toString(), 0);
      default:
        throw new IllegalStateException("Unexpected value: " + node.getOperation());
    }
  }

  private static Object parseValue(RecordStoreProtocol.FieldNode node) throws ParseException {
    switch (node.getValueCase()) {
      case STRING_VALUE:
        return node.getStringValue();
      case INT32_VALUE:
        return node.getInt32Value();
      case INT64_VALUE:
        return node.getInt64Value();
      case FLOAT_VALUE:
        return node.getFloatValue();
      case UINT32_VALUE:
        return node.getUint32Value();
      case UINT64_VALUE:
        return node.getUint64Value();
      case SINT32_VALUE:
        return node.getSint32Value();
      case SINT64_VALUE:
        return node.getSint64Value();
      case DOUBLE_VALUE:
        return node.getDoubleValue();
      case BOOL_VALUE:
        return node.getBoolValue();
      case BYTES_VALUE:
        return node.getBytesValue();
      case VALUE_NOT_SET:
        throw new ParseException("Value not set", 0);
    }
    throw new ParseException("something went wrong", 0);
  }
}
