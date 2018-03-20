package org.heigit.bigspatialdata.oshdb.util.geometry.osmtestdata;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class OSHDBGeometryBuilderTestOsmTestData1xx {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmTestData1xx() {
    testData.add("./src/test/resources/osm-testdata/all.osm");
  }

  @Test
  public void test100() {
    // A single node
    OSMEntity entity = testData.nodes().get(100000L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    assertEquals(1.02, ((Point) result).getX(), DELTA);
    assertEquals(1.02, ((Point) result).getY(), DELTA);
  }

  @Test
  public void test114() {
    // Two ways connected end-to-beginning
    OSMEntity entity1 = testData.ways().get(114800L).get(0);
    OSMEntity entity2 = testData.ways().get(114801L).get(0);
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp, areaDecider);
    assertTrue(result1 instanceof LineString);
    assertTrue(result2 instanceof LineString);
    assertEquals(
        ((LineString) result1).getCoordinateN(result1.getNumPoints()-1),
        ((LineString) result2).getCoordinateN(0)
    );
  }

}
