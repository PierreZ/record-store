package fr.pierrezemb.recordstore.grpc;

import io.grpc.Metadata;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

public class GrpcMetadataKeys {
  public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> TENANT_METADATA_KEY = Metadata.Key.of("Tenant", ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> RECORDSPACE_METADATA_KEY = Metadata.Key.of("RecordSpace", ASCII_STRING_MARSHALLER);
}
