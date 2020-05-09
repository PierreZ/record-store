package fr.pierrezemb.recordstore.presto;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.prestosql.spi.type.TypeManager;

public class RSModule implements Module {
  public RSModule(String catalogName, TypeManager typeManager) {
  }

  @Override
  public void configure(Binder binder) {

  }
}
