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

  public void LoadDataset(List<DemoDatasetEnum> datasets) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
    for (DemoDatasetEnum d : datasets) {
      Dataset dataset;
      switch (d) {
        case PERSONS:
          dataset = new PersonDataset();
          break;
        default:
          continue;
      }
      dataset.load(recordLayer, DEFAULT_DEMO_TENANT, d.toString(), this.nbrRecords);
      LOGGER.info("successfully loaded {} records for {}", this.nbrRecords, d.toString());
    }
  }

  public void LoadDataset(String datasetsToLoad) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {

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
      datasetsLoader.LoadDataset(datasets);
    }
  }
}
