package fr.pierrezemb.recordstore.utils;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;

public class RecordQueryGenerator {
  public static RecordQuery generate(RecordStoreProtocol.QueryRequest request) {
    RecordQuery.Builder queryBuilder = RecordQuery.newBuilder()
      .setRecordType(request.getTable());

    return parseNode(request.getQueryNode(), queryBuilder);
  }

  public static RecordQuery parseNode(RecordStoreProtocol.Node node, RecordQuery.Builder queryBuilder) {
    switch (node.getContentCase()) {

      case FIELD_NODE:
        parseFieldNode(node.getFieldNode(), queryBuilder);
      case AND_NODE:
        break;
      case OR_NODE:
        break;
      case CONTENT_NOT_SET:
        break;
    }

    return queryBuilder.build();

  }

  private static void parseFieldNode(RecordStoreProtocol.FieldNode node, RecordQuery.Builder queryBuilder) {
    switch (node.getOperation()) {

      case GREATER_THAN_OR_EQUALS:
        break;
      case LESS_THAN_OR_EQUALS:
        break;
      case EQUALS:
        parseFieldNodeEquals(node, queryBuilder);
      case UNRECOGNIZED:
        break;
    }
  }

  private static void parseFieldNodeEquals(RecordStoreProtocol.FieldNode node, RecordQuery.Builder queryBuilder) {
    switch (node.getValueCase()) {
      case STRING_VALUE:
        queryBuilder.setFilter(Query.field(node.getField()).equalsValue(node.getStringValue()));
        break;
      case INT64_VALUE:
        break;
      case BYTES_VALUE:
        break;
      case VALUE_NOT_SET:
        break;
    }
  }
}
