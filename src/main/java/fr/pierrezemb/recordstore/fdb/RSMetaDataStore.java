package fr.pierrezemb.recordstore.fdb;

import com.apple.foundationdb.record.provider.foundationdb.FDBMetaDataStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

public class RSMetaDataStore {
  public static FDBMetaDataStore createMetadataStore(FDBRecordContext context, String tenant) {
    FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, RSKeySpace.getMetaDataKeySpacePath(tenant));
    metaDataStore.setMaintainHistory(true);
    return metaDataStore;
  }
}
