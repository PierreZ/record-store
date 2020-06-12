package fr.pierrezemb.recordstore.client;

public class RecordQuery {
  public static RecordField field(String name) {
    return new RecordField(name);
  }
}
