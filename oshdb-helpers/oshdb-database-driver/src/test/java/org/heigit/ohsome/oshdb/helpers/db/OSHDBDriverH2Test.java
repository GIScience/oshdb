package org.heigit.ohsome.oshdb.helpers.db;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OSHDBDriverH2Test {

  private static final Properties props = new Properties();

  public OSHDBDriverH2Test() {
    props.setProperty("oshdb", "h2:../../data/${test-file}");
    // relevant for getter test
    props.setProperty("test-file", "test-data");
  }

  @Test
  @DisplayName("OSHDBConnection getSnapshotView")
  void getSnapshotView() throws Exception {
    int queryResult = OSHDBDriver.connect(props, oshdb -> {
      var bbox = bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);

      return oshdb.getSnapshotView()
          .areaOfInterest(bbox)
          .filter("type:node")
          .timestamps("2018-05-01")
          .count();
    });
    assertEquals(7, queryResult);
  }

  @Test
  @DisplayName("OSHDBConnection getContributionView")
  void getContributionView() throws Exception {
    int queryResult = OSHDBDriver.connect(props, oshdb -> {
      var bbox = bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);

      return oshdb.getContributionView()
          .areaOfInterest(bbox)
          .filter("type:node")
          .timestamps("2007-11-01", "2019-01-01")
          .count();
    });
    assertEquals(16, queryResult);
  }

  @Test
  @DisplayName("OSHDBConnection Getter")
  void getter() throws Exception {
    OSHDBDriver.connect(props, oshdb -> {
      assertNotNull(oshdb.getProps());
      assertNotNull(oshdb.getOSHDB());
      assertNotNull(oshdb.getKeytables());
      assertNotNull(oshdb.getTagTranslator());
      return 0;
    });
  }
}