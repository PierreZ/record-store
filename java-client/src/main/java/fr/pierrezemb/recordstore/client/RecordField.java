package fr.pierrezemb.recordstore.client;

import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;

public class RecordField {
  @Nonnull
  private final String fieldName;

  public RecordField(@NotNull String fieldName) {
    this.fieldName = fieldName;
  }

  public RecordStoreProtocol.QueryFilterNode lessThan(int value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThan(double value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThan(long value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThan(float value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThanOrEquals(int value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThanOrEquals(double value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThanOrEquals(long value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode lessThanOrEquals(float value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.LESS_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThan(int value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThan(double value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThan(long value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThan(float value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThanOrEquals(int value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThanOrEquals(double value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThanOrEquals(long value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS, value);
  }

  public RecordStoreProtocol.QueryFilterNode greaterThanOrEquals(float value) {
    return createQueryFilter(RecordStoreProtocol.FieldOperation.GREATER_THAN_OR_EQUALS, value);
  }

  private RecordStoreProtocol.QueryFilterNode createQueryFilter(RecordStoreProtocol.FieldOperation fieldOperation, Object value) {
    return RecordStoreProtocol.QueryFilterNode.newBuilder()
      .setFieldNode(createQueryFilterFieldNode(fieldOperation, value))
      .build();
  }

  private RecordStoreProtocol.QueryFilterFieldNode createQueryFilterFieldNode(RecordStoreProtocol.FieldOperation fieldOperation, Object value) {
    RecordStoreProtocol.QueryFilterFieldNode.Builder builder = RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
      .setField(fieldName)
      .setOperation(fieldOperation);

    if (value instanceof Integer) {
      builder.setInt32Value((Integer) value);
    }

    if (value instanceof Double) {
      builder.setDoubleValue((Double) value);
    }

    if (value instanceof Long) {
      builder.setInt64Value((Long) value);
    }

    if (value instanceof Float) {
      builder.setFloatValue((Float) value);
    }

    return builder.build();
  }

}
