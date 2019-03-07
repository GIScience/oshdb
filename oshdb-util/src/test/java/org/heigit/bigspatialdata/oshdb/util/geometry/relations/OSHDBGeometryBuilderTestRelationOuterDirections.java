package org.heigit.bigspatialdata.oshdb.util.geometry.relations;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTestRelationOuterDirections {

  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestRelationOuterDirections() {
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
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
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
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
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
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
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
        "MULTIPOLYGON(((7.16 1.36,7.16 1.35,7.15 1.34,7.14 1.34,7.14 1.35,7.14 1.36,7.15 1.36,7.15 1.37,7.16 1.37,7.16 1.36)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
  }
}