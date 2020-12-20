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
package fr.pierrezemb.recordstore;

import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;

public class Launcher extends io.vertx.core.Launcher {

  public static void main(String[] args) {
    new Launcher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    options.setMetricsOptions(
        new MicrometerMetricsOptions()
            .setPrometheusOptions(
                new VertxPrometheusOptions()
                    .setEnabled(true)
                    .setStartEmbeddedServer(true)
                    .setEmbeddedServerOptions(
                        new HttpServerOptions().setHost("127.0.0.1").setPort(9098))
                    .setEmbeddedServerEndpoint("/metrics"))
            .setEnabled(true));
    System.out.println("starting metrics on 9098");
  }
}
