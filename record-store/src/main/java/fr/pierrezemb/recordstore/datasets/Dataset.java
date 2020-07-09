package fr.pierrezemb.recordstore.datasets;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.fdb.RecordLayer;

public interface Dataset {
  void load(RecordLayer recordLayer, String tenant, String recordSpace, int nbrRecord) throws Descriptors.DescriptorValidationException, InvalidProtocolBufferException;
}
