package org.heigit.ohsome.oshdb.helpers.db;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OSHDBDriverH2Test {

  private static final Properties props = new Properties();

  public OSHDBDriverH2Test() {
    props.setProperty("oshdb", "h2:../../data/${test-file}");
    // relevant for getter test
    props.setProperty("test-file", "test-data");
  }

  @SuppressWarnings("ConstantConditions")
  private static int testGetters(OSHDBConnection oshdb) {
    assertTrue(oshdb.getProps() instanceof Properties);
    assertTrue(oshdb.getOSHDB() instanceof OSHDBDatabase);
    assertTrue(oshdb.getTagTranslator() instanceof TagTranslator);
    return 0;
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
    OSHDBDriver.connect(props, OSHDBDriverH2Test::testGetters);
  }
}