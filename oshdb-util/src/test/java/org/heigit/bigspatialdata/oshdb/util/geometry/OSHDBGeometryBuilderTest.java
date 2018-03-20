package org.heigit.bigspatialdata.oshdb.util.geometry;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaAlways;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.heigit.bigspatialdata.oshdb.util.time.ISODateTimeParser;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTest() {
    testData.add("./src/test/resources/geometryBuilder.osh");
  }

  private OSHDBTimestamp toOSHDBTimestamp(String timeString) {
    try {
      return new OSHDBTimestamp(
          ISODateTimeParser.parseISODateTime(timeString).toEpochSecond()
      );
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
  @Test
  public void testPointGetGeometry() {
    OSMEntity entity = testData.nodes().get(1L).get(1);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, null);
    assertTrue(result instanceof Point);
    assertEquals(100, result.getCoordinates()[0].x, DELTA);
    assertEquals(80, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  public void testPointGetGeometryClipped() {
    OSMEntity entity = testData.nodes().get(1L).get(1);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    // by bbox
    OSHDBBoundingBox clipBbox = new OSHDBBoundingBox(-180.0, -90.0, 180.0, 90.0);
    Geometry result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipBbox);
    assertEquals(false, result.isEmpty());
    clipBbox = new OSHDBBoundingBox(-180.0, -90.0, 0.0, 0.0);
    result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipBbox);
    assertEquals(true, result.isEmpty());
    // by poly
    Polygon clipPoly = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(-180, -90, 180, 90));
    result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipPoly);
    assertEquals(false, result.isEmpty());
    clipPoly = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(-1, -1, 1, 1));
    result = OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, null, clipPoly);
    assertEquals(true, result.isEmpty());
  }

  @Test
  public void testWayGetGeometry() {
    // linestring
    OSMEntity entity = testData.ways().get(1L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaNever();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("LineString", result.getGeometryType());
    assertEquals(2, result.getNumPoints());
    assertEquals(100, result.getCoordinates()[0].x, DELTA);
    assertEquals(80, result.getCoordinates()[0].y, DELTA);
    assertEquals(110, result.getCoordinates()[1].x, DELTA);
    assertEquals(80.1, result.getCoordinates()[1].y, DELTA);

    // polygon
    entity = testData.ways().get(2L).get(0);
    areaDecider = new FakeTagInterpreterAreaAlways();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(5, result.getNumPoints());
    assertEquals(result.getCoordinates()[0].x, result.getCoordinates()[4].x, DELTA);
    assertEquals(result.getCoordinates()[0].y, result.getCoordinates()[4].y, DELTA);

    // other timestamp -> changed member
    entity = testData.ways().get(1L).get(0);
    timestamp = TimestampParser.toOSHDBTimestamp("2003-01-01");
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals(80.2, result.getCoordinates()[0].y, DELTA);
  }

  @Test
  public void testWayGetGeometryIncomplete() {
    // linestring with 3 node references, only 2 present
    OSMEntity entity = testData.ways().get(3L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaNever();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("LineString", result.getGeometryType());
    assertEquals(2, result.getNumPoints());

    // single noded way
    entity = testData.ways().get(4L).get(0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Point", result.getGeometryType());
    assertEquals(false, result.isEmpty());

    // zero noded way
    entity = testData.ways().get(5L).get(0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals(true, result.isEmpty());
  }

  @Test
  public void testRelationGetGeometry() {
    // simplest multipolygon
    OSMEntity entity = testData.relations().get(1L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(5, result.getNumPoints());

    // other relation
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("GeometryCollection", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());
    assertEquals(5, result.getGeometryN(0).getNumPoints());
  }

  @Test
  public void testRelationGetGeometryIncomplete() {
    // multipolygon, missing ring
    OSMEntity entity = testData.relations().get(2L).get(0);
    OSHDBTimestamp timestamp = TimestampParser.toOSHDBTimestamp("2001-01-01");
    TagInterpreter areaDecider = new FakeTagInterpreterAreaMultipolygonAllOuters();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("Polygon", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());

    // multipolygon, incomplete ring
    entity = testData.relations().get(3L).get(0);
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("GeometryCollection", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());

    // other relation, missing member
    areaDecider = new FakeTagInterpreterAreaNever();
    result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals("GeometryCollection", result.getGeometryType());
    assertEquals(1, result.getNumGeometries());
  }
  
  @Test
  public void testBoundingBoxGetGeometry() {
    // regular bbox
    OSHDBBoundingBox bbox = new OSHDBBoundingBox(0, 0, 1, 1);
    Polygon geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    Coordinate[] test = { new Coordinate(0, 0), new Coordinate(1, 0), new Coordinate(1, 1),
        new Coordinate(0, 1), new Coordinate(0, 0) };
    Assert.assertArrayEquals(test, geometry.getCoordinates());

    // degenerate bbox: point
    bbox = new OSHDBBoundingBox(0, 0, 0, 0);
    geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    test = new Coordinate[] { new Coordinate(0, 0), new Coordinate(0, 0), new Coordinate(0, 0),
        new Coordinate(0, 0), new Coordinate(0, 0) };
    Assert.assertArrayEquals(test, geometry.getCoordinates());

    // degenerate bbox: line
    bbox = new OSHDBBoundingBox(0, 0, 0, 1);
    geometry = OSHDBGeometryBuilder.getGeometry(bbox);
    test = new Coordinate[]{new Coordinate(0, 0), new Coordinate(0, 0), new Coordinate(0, 1),
        new Coordinate(0, 1), new Coordinate(0, 0) };
    Assert.assertArrayEquals(test, geometry.getCoordinates());
  }
}


abstract class FakeTagInterpreter implements TagInterpreter {
  @Override
  public boolean isArea(OSMEntity entity) { return false; }
  @Override
  public boolean isLine(OSMEntity entity) { return false; }
  @Override
  public boolean hasInterestingTagKey(OSMEntity osm) { return false; }
  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) { return false; }
  @Override
  public boolean isMultipolygonInnerMember(OSMMember osmMember) { return false; }
  @Override
  public boolean isOldStyleMultipolygon(OSMRelation osmRelation) { return false; }
}

class FakeTagInterpreterAreaAlways extends FakeTagInterpreter {
  @Override
  public boolean isArea(OSMEntity e) {
    if (e instanceof OSMWay) {
      OSMMember[] nds = ((OSMWay) e).getRefs();
      return (nds.length >= 4 && nds[0].getId() == nds[nds.length - 1].getId());
    }
    return true;
  }
}

class FakeTagInterpreterAreaMultipolygonAllOuters extends FakeTagInterpreterAreaAlways {
  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    return true;
  }
}

class FakeTagInterpreterAreaNever extends FakeTagInterpreter {
  @Override
  public boolean isArea(OSMEntity e) {
    return false;
  }
}
