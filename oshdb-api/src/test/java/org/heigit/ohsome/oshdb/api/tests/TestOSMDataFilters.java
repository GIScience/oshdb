package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests osm data filters.
 */
class TestOSMDataFilters {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");

  TestOSMDataFilters() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private OSMEntitySnapshotView createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.view();
  }

  // filter: area of interest

  @Test
  void bbox() throws Exception {
    Long result = createMapReducerOSMEntitySnapshot()
        .filter("type:node")
        .areaOfInterest(bbox)
        .timestamps(timestamps1)
        .on(oshdb)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  void polygon() throws Exception {
    Long result = createMapReducerOSMEntitySnapshot()
        .filter("type:node")
        .areaOfInterest(OSHDBGeometryBuilder.getGeometry(bbox))
        .timestamps(timestamps1)
        .on(oshdb)
        .count();
    assertEquals(2, result.intValue());
  }

  @Test
  void multiPolygon() throws Exception {
    GeometryFactory gf = new GeometryFactory();
    Long result = createMapReducerOSMEntitySnapshot()
        .filter("type:node")
        .areaOfInterest(gf.createMultiPolygon(new Polygon[] {
            OSHDBGeometryBuilder.getGeometry(bbox)
        }))
        .timestamps(timestamps1)
        .on(oshdb)
        .count();
    assertEquals(2, result.intValue());
  }
}
