package org.heigit.bigspatialdata.oshdb.util.geometry.incomplete;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class OSHDBGeometryBuilderTestPolygonIncompleteDataTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestPolygonIncompleteDataTest() {
    testData.add("./src/test/resources/incomplete-osm/polygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void testSomeNodesOfWayNotExistent() throws ParseException {
    // Valid multipolygon relation with two ways making up an outer ring, in second ring 2 node
    // references to not existing nodes
    //TODO https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/138
    OSMEntity entity = testData.relations().get(500L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(7, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01,7.31 1.01,7.33 1.04,7.32 1.04,7.32 1.05,7.34 1.05,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void testWayNotExistent() throws ParseException {
    // Valid multipolygon relation with two way references, one way does not exist
    //TODO https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/138
    OSMEntity entity = testData.relations().get(501L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());

    assertEquals(6, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.04, 7.33 1.05, 7.33 1.04, 7.32 1.04, 7.31 1.01,7.31 1.01,"
            + "7.31 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void testAllNodesOfWayNotExistent() {
    // relation with one way with two nodes, both missing
    OSMEntity entity1 = testData.relations().get(502L).get(0);
    Geometry result1 = null;
    try {
      result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }
}
