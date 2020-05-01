package fr.pierrezemb.recordstore.grpc;

import com.clevercloud.biscuit.error.Error;
import com.google.common.collect.ImmutableMap;
import fr.pierrezemb.recordstore.auth.BiscuitManager;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.vavr.control.Either;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthInterceptor implements ServerInterceptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthInterceptor.class);
  private static final Map<Metadata.Key<String>, Context.Key<String>> METADATA_KEY_TO_CONTEXT_KEY = ImmutableMap.of(
    GrpcMetadataKeys.CONTAINER_METADATA_KEY, GrpcContextKeys.CONTAINER_NAME,
    GrpcMetadataKeys.TENANT_METADATA_KEY, GrpcContextKeys.TENANT_ID_KEY
  );
  private final BiscuitManager biscuitManager;
  private final List<Metadata.Key<String>> requiredKeys = Arrays.asList(
    GrpcMetadataKeys.AUTHORIZATION_METADATA_KEY,
    GrpcMetadataKeys.CONTAINER_METADATA_KEY,
    GrpcMetadataKeys.TENANT_METADATA_KEY
  );

  public AuthInterceptor(String key) {
    biscuitManager = new BiscuitManager(key);
  }

  /**
   * Intercept {@link ServerCall} dispatch by the {@code next} {@link ServerCallHandler}. General
   * semantics of {@link ServerCallHandler#startCall} apply and the returned
   * {@link ServerCall.Listener} must not be {@code null}.
   *
   * <p>If the implementation throws an exception, {@code call} will be closed with an error.
   * Implementations must not throw an exception if they started processing that may use {@code
   * call} on another thread.
   *
   * @param call    object to receive response messages
   * @param headers which can contain extra call metadata from {@link ClientCall#start},
   *                e.g. authentication credentials.
   * @param next    next processor in the interceptor chain
   * @return listener for processing incoming messages for {@code call}, never {@code null}.
   */
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    Context context = Context.current();

    for (Metadata.Key<String> requiredKey : requiredKeys) {
      if (headers.containsKey(requiredKey)) {
        String value = headers.get(requiredKey);

        if (METADATA_KEY_TO_CONTEXT_KEY.containsKey(requiredKey)) {
          // adding key to metadata
          context = context.withValue(METADATA_KEY_TO_CONTEXT_KEY.get(requiredKey), value);
        }

      } else {
        call.close(Status.UNAUTHENTICATED.withDescription(requiredKey.toString() + " not passed as metadata"), headers);
        return new ServerCall.Listener<ReqT>() {
        };
      }
    }

    String token = headers.get(GrpcMetadataKeys.AUTHORIZATION_METADATA_KEY);
    String tenant = headers.get(GrpcMetadataKeys.TENANT_METADATA_KEY);
    Either<Error, Void> result = this.biscuitManager.checkTenant(tenant, token.substring("Bearer ".length()));
    if (result.isLeft()) {
      call.close(Status.UNAUTHENTICATED.withDescription("bad tenant and/or token"), headers);
      return new ServerCall.Listener<ReqT>() {
      };
    }

    return Contexts.interceptCall(context, call, headers, next);
  }
}
