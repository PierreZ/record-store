package fr.pierrezemb.recordstore.utils;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RecordQueryGenerator {

  public static RecordQuery generate(RecordStoreProtocol.QueryRequest request) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType(request.getTable());

    try {
      QueryComponent queryComponents = parseNode(request.getQueryNode());
      return queryBuilder.setFilter(queryComponents).build();
    } catch (ParseException e) {
      throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withCause(e));
    }
  }

  public static RecordQuery generate(RecordStoreProtocol.DeleteRecordRequest request) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType(request.getTable());

    try {
      QueryComponent queryComponents = parseNode(request.getQueryNode());
      return queryBuilder.setFilter(queryComponents).build();
    } catch (ParseException e) {
      System.err.println(e);
    }
    return null;
  }

  public static QueryComponent parseNode(RecordStoreProtocol.Node node) throws ParseException {
    switch (node.getContentCase()) {

      case FIELD_NODE:
        return parseFieldNode(node.getFieldNode());
      case AND_NODE:
        return Query.and(parseChildrenNodes(node.getAndNode()));
      case OR_NODE:
        return Query.or(parseChildrenNodes(node.getOrNode()));
      case CONTENT_NOT_SET:
        throw new ParseException("no content sent on node " + node.toString(), 0);
    }

    return null;

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
    switch (node.getOperation()) {
      case GREATER_THAN_OR_EQUALS:
        return Query.field(node.getField()).greaterThanOrEquals(parseValue(node));
      case LESS_THAN_OR_EQUALS:
        return Query.field(node.getField()).lessThanOrEquals(parseValue(node));
      case GREATER_THAN:
        return Query.field(node.getField()).greaterThan(parseValue(node));
      case LESS_THAN:
        return Query.field(node.getField()).lessThan(parseValue(node));
      case START_WITH:
        return Query.field(node.getField()).startsWith(String.valueOf(parseValue(node)));
      case IS_EMPTY:
        return Query.field(node.getField()).isEmpty();
      case IS_NULL:
        return Query.field(node.getField()).isNull();
      case EQUALS:
        return Query.field(node.getField()).equalsValue(parseValue(node));
      case NOT_EQUALS:
        return Query.field(node.getField()).notEquals(parseValue(node));
      case NOT_NULL:
        return Query.field(node.getField()).notNull();
      case MATCHES:
        if (node.getValueCase() != RecordStoreProtocol.FieldNode.ValueCase.FIELDNODE) {
          throw new ParseException("Matches onl accept a nested FieldValue", 0);
        }
        return Query.field(node.getField()).matches(Objects.requireNonNull(parseFieldNode(node.getFieldNode())));
      case UNRECOGNIZED:
        throw new ParseException("unrecognized field on node " + node.toString(), 0);
    }
    return null;
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
