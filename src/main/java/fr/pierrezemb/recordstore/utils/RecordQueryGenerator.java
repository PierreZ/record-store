package fr.pierrezemb.recordstore.utils;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class RecordQueryGenerator {
  public static RecordQuery generate(RecordStoreProtocol.QueryRequest request) {
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
        break;
      case CONTENT_NOT_SET:
        break;
    }

    return null;

  }

  private static List<QueryComponent> parseChildrenNodes(RecordStoreProtocol.AndNode node) throws ParseException {
    List<QueryComponent> queryComponents = new ArrayList<>();
    for (RecordStoreProtocol.Node children : node.getNodesList()) {
      queryComponents.add(parseNode(children));
    }
    return queryComponents;
  }

  private static QueryComponent parseFieldNode(RecordStoreProtocol.FieldNode node) throws ParseException {
    switch (node.getOperation()) {

      case GREATER_THAN_OR_EQUALS:
        return parseGreaterThanOrEquals(node);
      case LESS_THAN_OR_EQUALS:
        return parseLessThanOrEquals(node);
      case EQUALS:
        return parseFieldNodeEquals(node);
      case UNRECOGNIZED:
        break;
    }
    return null;
  }

  private static QueryComponent parseLessThanOrEquals(RecordStoreProtocol.FieldNode node) throws ParseException {
    switch (node.getValueCase()) {
      case STRING_VALUE:
        return Query.field(node.getField()).lessThanOrEquals(node.getStringValue());
      case INT64_VALUE:
        return Query.field(node.getField()).lessThanOrEquals(node.getInt64Value());
      case BYTES_VALUE:
        return Query.field(node.getField()).lessThanOrEquals(node.getBytesValue());
      case VALUE_NOT_SET:
        throw new ParseException("Value not set", 0);
    }
    return null;
  }

  private static QueryComponent parseGreaterThanOrEquals(RecordStoreProtocol.FieldNode node) throws ParseException {
    switch (node.getValueCase()) {
      case STRING_VALUE:
        return Query.field(node.getField()).greaterThanOrEquals(node.getStringValue());
      case INT64_VALUE:
        return Query.field(node.getField()).greaterThanOrEquals(node.getInt64Value());
      case BYTES_VALUE:
        return Query.field(node.getField()).greaterThanOrEquals(node.getBytesValue());
      case VALUE_NOT_SET:
        throw new ParseException("Value not set", 0);
    }
    return null;
  }

  private static QueryComponent parseFieldNodeEquals(RecordStoreProtocol.FieldNode node) throws ParseException {
    switch (node.getValueCase()) {
      case STRING_VALUE:
        return Query.field(node.getField()).equalsValue(node.getStringValue());
      case INT64_VALUE:
        return Query.field(node.getField()).equalsValue(node.getInt64Value());
      case BYTES_VALUE:
        return Query.field(node.getField()).equalsValue(node.getBytesValue());
      case VALUE_NOT_SET:
        throw new ParseException("Value not set", 0);
    }
    return null;
  }
}
