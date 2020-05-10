package fr.pierrezemb.recordstore.presto;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.prestosql.spi.type.TypeManager;

public class RecordStoreModule implements Module {
  public RecordStoreModule(String catalogName, TypeManager typeManager) {
  }

  @Override
  public void configure(Binder binder) {

  }
}
