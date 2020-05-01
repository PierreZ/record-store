package fr.pierrezemb.recordstore.grpc;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;

public class GrpcMetadataKeys {
  public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> TENANT_METADATA_KEY = Metadata.Key.of("Tenant", ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> CONTAINER_METADATA_KEY = Metadata.Key.of("Container", ASCII_STRING_MARSHALLER);
}
