/**
 * Copyright 2020 Pierre Zemb
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.pierrezemb.recordstore.grpc;

import fr.pierrezemb.recordstore.fdb.RecordLayer;
import fr.pierrezemb.recordstore.proto.AdminServiceGrpc;
import fr.pierrezemb.recordstore.proto.RecordStoreProtocol;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminService extends AdminServiceGrpc.AdminServiceImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminService.class);
  private final RecordLayer recordLayer;

  public AdminService(RecordLayer recordLayer) {
    this.recordLayer = recordLayer;
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void list(
      RecordStoreProtocol.ListContainerRequest request,
      StreamObserver<RecordStoreProtocol.ListContainerResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    List<String> results;
    try {
      results = recordLayer.listContainers(tenantID);
    } catch (RuntimeException e) {
      LOGGER.error("cannot list recordSpaces: {}", e);
      throw new StatusRuntimeException(Status.INTERNAL.withCause(e));
    }

    responseObserver.onNext(
        RecordStoreProtocol.ListContainerResponse.newBuilder().addAllContainers(results).build());
    responseObserver.onCompleted();
  }

  /**
   * @param request
   * @param responseObserver
   */
  @Override
  public void delete(
      RecordStoreProtocol.DeleteContainerRequest request,
      StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    String tenantID = GrpcContextKeys.getTenantIDOrFail();

    try {
      for (String recordSpace : request.getContainersList()) {
        recordLayer.deleteContainer(tenantID, recordSpace);
      }
    } catch (RuntimeException runtimeException) {
      LOGGER.error("could not delete recordSpace", runtimeException);
      throw new StatusRuntimeException(
          Status.INTERNAL.withDescription(runtimeException.getMessage()));
    }

    responseObserver.onNext(RecordStoreProtocol.EmptyResponse.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void ping(
      RecordStoreProtocol.EmptyRequest request,
      StreamObserver<RecordStoreProtocol.EmptyResponse> responseObserver) {
    GrpcContextKeys.getTenantIDOrFail();
    responseObserver.onNext(RecordStoreProtocol.EmptyResponse.newBuilder().build());
    responseObserver.onCompleted();
  }
}
