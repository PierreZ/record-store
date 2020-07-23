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
package fr.pierrezemb.recordstore.datasets;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import fr.pierrezemb.recordstore.fdb.RecordLayer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DatasetsLoader {
  public static final String DEFAULT_DEMO_TENANT = "demo";
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetsLoader.class);
  private final RecordLayer recordLayer;
  private final int nbrRecords;

  public DatasetsLoader(RecordLayer recordLayer) {
    this.recordLayer = recordLayer;
    nbrRecords = 100;
  }

  public void loadDataset(List<DemoDatasetEnum> datasets) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
    List<String> alreadyLoadedDatasets = this.recordLayer.listContainers(DEFAULT_DEMO_TENANT);
    for (DemoDatasetEnum d : datasets) {
      Dataset dataset;
      switch (d) {
        case USER:
          dataset = new UserDataset();
          break;
        default:
          continue;
      }
      if (!alreadyLoadedDatasets.contains(d.toString())) {
        dataset.load(recordLayer, DEFAULT_DEMO_TENANT, d.toString(), this.nbrRecords);
        LOGGER.info("successfully loaded {} records for {}", this.nbrRecords, d.toString());
      } else {
        LOGGER.info("dataset {} already loaded, skipping", d.toString());
      }
    }
  }

  public void loadDataset(String datasetsToLoad) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {

    if (datasetsToLoad == null) {
      return;
    }

    if (datasetsToLoad.length() > 0) {
      String[] d = datasetsToLoad.split(",");
      DatasetsLoader datasetsLoader = new DatasetsLoader(recordLayer);

      ArrayList<DemoDatasetEnum> datasets = new ArrayList<>();
      for (String dataset : d) {
        try {
          datasets.add(DemoDatasetEnum.valueOf(dataset.toUpperCase()));
        } catch (IllegalArgumentException e) {
          LOGGER.error("cannot find dataset {}", dataset);
        }
      }
      datasetsLoader.loadDataset(datasets);
    }
  }
}
