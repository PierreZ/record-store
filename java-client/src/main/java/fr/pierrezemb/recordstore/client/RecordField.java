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
    return RecordStoreProtocol.QueryFilterNode.newBuilder()
      .setFieldNode(
        RecordStoreProtocol.QueryFilterFieldNode.newBuilder()
          .setField(fieldName)
          .setOperation(RecordStoreProtocol.FieldOperation.LESS_THAN)
          .setInt64Value(value).build()
      )
      .build();
  }

}
