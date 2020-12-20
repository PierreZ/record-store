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
package org.testcontainers.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.Wait;

public class FoundationDBContainer extends GenericContainer<FoundationDBContainer> {
  public static final int FDB_PORT = 4500;
  private static final String FDB_VERSION = "6.2.19";
  private static final String FDB_IMAGE = "foundationdb/foundationdb";
  private static final Logger log = LoggerFactory.getLogger(FoundationDBContainer.class);
  private File clusterFile;

  public FoundationDBContainer() {
    this(FDB_VERSION);
  }

  public FoundationDBContainer(String fdbVersion) {
    super(FDB_IMAGE + ":" + fdbVersion);
    withExposedPorts(FDB_PORT);
    waitingFor(Wait.forListeningPort());
  }

  @Override
  protected void containerIsStarted(InspectContainerResponse containerInfo) {
    try {
      Container.ExecResult initResult =
          execInContainer("fdbcli", "--exec", "configure new single memory");
      String stdout = initResult.getStdout();
      log.debug("init FDB stdout: " + stdout);
      int exitCode = initResult.getExitCode();
      log.debug("init FDB exit code: " + exitCode);

      boolean fdbReady = false;
      log.info("waiting for FDB to be healthy");

      // waiting for fdb to be up and healthy
      while (!fdbReady) {

        Container.ExecResult statusResult = execInContainer("fdbcli", "--exec", "status");
        stdout = statusResult.getStdout();

        if (stdout.contains("Healthy")) {
          fdbReady = true;
          log.info("fdb is healthy");
        } else {
          log.debug("fdb is unhealthy");
          Thread.sleep(10 * 1000);
        }
      }
      clusterFile = File.createTempFile("fdb", ".cluster");
      copyFileFromContainer("/var/fdb/fdb.cluster", clusterFile.getAbsolutePath());
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * A hook that is executed after the container is stopped with {@link #stop()}. Warning! This hook
   * won't be executed if the container is terminated during the JVM's shutdown hook or by Ryuk.
   *
   * @param containerInfo
   */
  @Override
  protected void containerIsStopped(InspectContainerResponse containerInfo) {
    super.containerIsStopped(containerInfo);
    clusterFile.delete();
  }

  public File getClusterFile() {
    return clusterFile;
  }
}
