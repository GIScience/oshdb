package org.heigit.ohsome.oshdb.util.geometry.relations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class OSHDBGeometryBuilderRelationOuterDirectionsTest {

  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderRelationOuterDirectionsTest() {
    testData.add("./src/test/resources/relations/outer-directions.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }


  @Test
  public void testFromPointTwoWaysGoingToDiffDirections() throws ParseException {
    // start of partial ring matches start of current line
    // from one point in outer ring two ways are going to different directions
    OSMEntity entity = testData.relations().get(1L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void testToPointTwoWaysPointingFromDiffDirections() throws ParseException {
    // end of partial ring matches end of current line
    // to one point in outer ring two ways are pointing from different directions
    OSMEntity entity = testData.relations().get(2L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void testStartMatchesEnd() throws ParseException {
    // start of partial ring matches end of current line
    //
    OSMEntity entity = testData.relations().get(3L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }

  @Test
  public void testEndMatchesStart() throws ParseException {
    // end of partial ring matches to start of current line
    //
    OSMEntity entity = testData.relations().get(4L).get(0);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, tagInterpreter);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(0, ((Polygon) result).getNumInteriorRing());

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 "
        + "1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
}