package org.heigit.bigspatialdata.oshdb.util.geometry.incomplete;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;

public class OSHDBGeometryBuilderTestPolygonIncompleteData {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestPolygonIncompleteData() {
    testData.add("./src/test/resources/incomplete-osm/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test1() throws ParseException {
    // Valid multipolygon relation with two ways (8 points) making up an outer ring, in second ring 2 node
    // references to not existing nodes
    OSMEntity entity = testData.relations().get(500L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01,7.31 1.01,7.33 1.04,7.32 1.04,7.32 1.05,7.34 1.05,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void test() throws ParseException {
    // Valid multipolygon relation with two way references, one way does not exist
    OSMEntity entity = testData.relations().get(501L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());
    assertEquals(9, result.getCoordinates().length, DELTA);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01,7.31 1.02,7.33 1.03,7.33 1.04,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

}
