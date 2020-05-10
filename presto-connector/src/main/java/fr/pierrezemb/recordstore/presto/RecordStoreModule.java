package fr.pierrezemb.recordstore.presto;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.prestosql.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class RecordStoreModule implements Module {
  private final String connectorId;
  private final TypeManager typeManager;
  public RecordStoreModule(String connectorId, TypeManager typeManager) {
    this.connectorId = requireNonNull(connectorId, "connector id is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");

  }


  @Override
  public void configure(Binder binder) {
    binder.bind(TypeManager.class).toInstance(typeManager);

    binder.bind(RecordStoreConnector.class).in(Scopes.SINGLETON);

    binder.bind(RecordStoreMetadata.class).in(Scopes.SINGLETON);
    binder.bind(RecordStoreSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(RecordStoreRecordSetProvider.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(RecordStoreConnectorConfig.class);
  }
}
