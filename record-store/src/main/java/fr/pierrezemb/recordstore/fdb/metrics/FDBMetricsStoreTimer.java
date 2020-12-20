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
package fr.pierrezemb.recordstore.fdb.metrics;

import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import io.micrometer.core.instrument.Metrics;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Set;
import javax.annotation.Nonnull;

public class FDBMetricsStoreTimer extends FDBStoreTimer {

  // I did not find how to enable properly metrics when firing tests
  // so trying to use metrics without SPI enabled triggers NPE
  private final boolean export;

  public FDBMetricsStoreTimer(boolean enableExport) {
    export = enableExport;
    if (export) {
      Metrics.addRegistry(BackendRegistries.getDefaultNow());
    }
  }

  /**
   * Record the amount of time each element in a set of events took to run. This applies the same
   * time difference to each event in the set.
   *
   * @param events the set of events being recorded
   * @param timeDifferenceNanos the time that the instrumented events took to run
   */
  @Override
  public void record(Set<Event> events, long timeDifferenceNanos) {
    if (export) {
      for (Event count : events) {
        Metrics.counter(buildClassname(count.name() + "_ns"), "log_key", count.logKey())
            .increment(timeDifferenceNanos);
      }
    }
    super.record(events, timeDifferenceNanos);
  }

  /**
   * Record the amount of time an event took to run. Subclasses can extend this to also update
   * metrics aggregation or monitoring services.
   *
   * @param event the event being recorded
   * @param timeDifferenceNanos the time that instrumented event took to run
   */
  @Override
  public void record(Event event, long timeDifferenceNanos) {
    if (export) {
      Metrics.counter(buildClassname(event.name() + "_ns"), "log_key", event.logKey())
          .increment(timeDifferenceNanos);
    }
    super.record(event, timeDifferenceNanos);
  }

  /**
   * Record that each event in a set occurred once. This increments the counters associated with
   * each event.
   *
   * @param events the set of events being recorded
   */
  @Override
  public void increment(@Nonnull Set<Count> events) {
    if (export) {
      for (Count count : events) {
        Metrics.counter(buildClassname(count.name()), "log_key", count.logKey()).increment();
      }
    }
    super.increment(events);
  }

  /**
   * Record that an event occurred once. This increments the counter associated with the given
   * event.
   *
   * @param event the event being recorded
   */
  @Override
  public void increment(@Nonnull Count event) {
    if (export) {
      Metrics.counter(buildClassname(event.name()), "log_key", event.logKey()).increment();
    }
    super.increment(event);
  }

  private String buildClassname(String name) {
    return "record_layer_" + name.toLowerCase();
  }

  /**
   * Record that each event occurred one or more times. This increments the counters associated with
   * each event by <code>amount</code>.
   *
   * @param events the set of events being recorded
   * @param amount the number of times each event occurred
   */
  @Override
  public void increment(@Nonnull Set<Count> events, int amount) {
    if (export) {
      for (Count count : events) {
        Metrics.counter(buildClassname(count.name()), "log_key", count.logKey()).increment(amount);
      }
    }
    super.increment(events, amount);
  }

  /**
   * Record that an event occurred one or more times. This increments the counter associated with
   * the given event by <code>amount</code>.
   *
   * @param event the event being recorded
   * @param amount the number of times the event occurred
   */
  @Override
  public void increment(Count event, int amount) {
    if (export) {
      Metrics.counter(buildClassname(event.name()), "log_key", event.logKey()).increment(amount);
    }
    super.increment(event, amount);
  }
}
