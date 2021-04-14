package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;


public class OSHDBGeometryBuilderTestOsmHistoryTestDataRelationTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataRelationTest() {
    testData.add("./src/test/resources/different-timestamps/polygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void testGeometryChange() throws ParseException {
    // relation getting more ways, one disappears, last version not valid
    OSMEntity entity = testData.relations().get(500L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(9, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(500L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof MultiPolygon);
    assertTrue(result1.isValid());
    assertEquals(14, result1.getCoordinates().length);
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)),"
            + "(( 7.32 1.05,7.32 1.07,7.31 1.07,7.31 1.05,7.32 1.05)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
    // third version
    OSMEntity entity2 = testData.relations().get(500L).get(2);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    try {
      Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp2, areaDecider);
      assertTrue(result2 instanceof GeometryCollection || result2 instanceof Polygonal);
      assertEquals(3, result2.getNumGeometries());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testVisibleChange() throws ParseException {
    // relation  visible tag changed
    OSMEntity entity = testData.relations().get(501L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.35 1.01, 7.34 1.01,7.34 1.02,7.35 1.02, 7.35 1.01)),"
            + "((7.33 1.04,7.33 1.03, 7.31 1.02, 7.31 1.04, 7.33 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(501L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1.isEmpty());
    // third version
    OSMEntity entity2 = testData.relations().get(501L).get(2);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof MultiPolygon);
    assertTrue(result2.isValid());
    assertEquals(10, result2.getCoordinates().length);
    Geometry expectedPolygon2 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.35 1.01, 7.34 1.01,7.34 1.02,7.35 1.02, 7.35 1.01)),"
               + "((7.33 1.04,7.33 1.03, 7.31 1.02, 7.31 1.04, 7.33 1.04)))"
    );
    Geometry intersection2 = result2.intersection(expectedPolygon2);
    assertEquals(expectedPolygon2.getArea(), intersection2.getArea(), DELTA);
  }

  @Test
  public void testWaysNotExistent() {
    // relation with two ways, both missing
    OSMEntity entity = testData.relations().get(502L).get(0);
    try {
      OSHDBTimestamp timestamp = entity.getTimestamp();
      OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTagChange() throws ParseException {
    // relation tags changing
    OSMEntity entity = testData.relations().get(503L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(503L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertTrue(result1.isValid());
    assertEquals(5, result1.getCoordinates().length);
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
    // third version
    OSMEntity entity2 = testData.relations().get(503L).get(2);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Polygon);
    assertTrue(result2.isValid());
    assertEquals(5, result2.getCoordinates().length);
    Geometry expectedPolygon2 = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    Geometry intersection2 = result2.intersection(expectedPolygon2);
    assertEquals(expectedPolygon2.getArea(), intersection2.getArea(), DELTA);
  }

  @Test
  public void testGeometryChangeOfNodeRefsInWays() throws ParseException {
    // relation, way 109 -inner- and 110 -outer- ways changed node refs
    OSMEntity entity = testData.relations().get(504L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(10, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.24 1.04, 7.24 1.07, 7.30 1.07, 7.30 1.04, 7.24 1.04),"
            + "(7.26 1.055, 7.265 1.06, 7.28 1.06,7.265 1.065, 7.26 1.055)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(504L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertTrue(result1.isValid());
    assertEquals(10, result1.getCoordinates().length);
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.24 1.04, 7.24 1.07, 7.30 1.07, 7.30 1.04, 7.24 1.04),"
            + "( 7.26 1.05,7.265 1.06, 7.28 1.06, 7.265 1.05,7.26 1.05)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
    // version in between
    OSMEntity entityBetween = testData.relations().get(504L).get(0);
    OSHDBTimestamp timestampBetween =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    Geometry resultBetween = OSHDBGeometryBuilder.getGeometry(entityBetween, timestampBetween,
        areaDecider);
    assertTrue(resultBetween instanceof Polygon);
    assertTrue(resultBetween.isValid());
    assertEquals(10, resultBetween.getCoordinates().length);
    Geometry expectedPolygonBetween = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.04, 7.24 1.07, 7.31 1.07, 7.31 1.04 , 7.24 1.04),"
            + "(7.26 1.055, 7.265 1.06, 7.28 1.06,7.265 1.065, 7.26 1.055)))"
    );
    Geometry intersectionBetween = resultBetween.intersection(expectedPolygonBetween);
    assertEquals(expectedPolygonBetween.getArea(), intersectionBetween.getArea(), DELTA);
  }

  @Test
  public void testGeometryChangeOfNodeCoordinatesInWay() throws ParseException {
    // relation, way 112  changed node coordinates
    OSMEntity entity = testData.relations().get(505L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.048, 7.245 1.072, 7.305 1.078, 7.303 1.042 , 7.24 1.048)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // version after
    OSMEntity entityAfter = testData.relations().get(505L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    assertTrue(resultAfter instanceof Polygon);
    assertTrue(resultAfter.isValid());
    assertEquals(5, resultAfter.getCoordinates().length);
    Geometry expectedPolygonAfter = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.042, 7.242 1.07, 7.305 1.07, 7.295 1.039 , 7.24 1.042)))"
    );
    Geometry intersectionAfter = resultAfter.intersection(expectedPolygonAfter);
    assertEquals(expectedPolygonAfter.getArea(), intersectionAfter.getArea(), DELTA);
  }

  @Test
  public void testGeometryChangeOfNodeCoordinatesInRelationAndWay() throws ParseException {
    // relation, with node members, nodes changed coordinates
    OSMEntity entity = testData.relations().get(506L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.048, 7.245 1.072,  7.303 1.042 , 7.24 1.048)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // version after
    OSMEntity entityAfter = testData.relations().get(506L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    assertTrue(resultAfter instanceof Polygon);
    assertTrue(resultAfter.isValid());
    assertEquals(4, resultAfter.getCoordinates().length);
    Geometry expectedPolygonAfter = (new WKTReader()).read(
        "MULTIPOLYGON(((7.24 1.042, 7.242 1.07, 7.295 1.039 , 7.24 1.042)))"
    );
    Geometry intersectionAfter = resultAfter.intersection(expectedPolygonAfter);
    assertEquals(expectedPolygonAfter.getArea(), intersectionAfter.getArea(), DELTA);
  }

  @Test
  public void testGeometryCollection() {
    // relation, not valid, should be a not empty geometryCollection
    // https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/143
    OSMEntity entity = testData.relations().get(507L).get(0);
    try {
      OSHDBTimestamp timestamp = entity.getTimestamp();
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertEquals(6, result.getNumGeometries());
      assertFalse(result instanceof MultiPolygon);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testNodesOfWaysNotExistent() {
    // relation with two ways, all nodes not existing
    OSMEntity entity = testData.relations().get(508L).get(0);
    try {
      OSHDBTimestamp timestamp = entity.getTimestamp();
      OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testVisibleChangeOfNodeInWay() throws ParseException {
    // relation, way member: node 52 changes visible tag
    OSMEntity entity = testData.relations().get(509L).get(0);
    // timestamp where node 52 visible is false
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.303 1.042, 7.32 1.07, 7.32 1.04,7.303 1.042)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // version after
    OSMEntity entityAfter = testData.relations().get(509L).get(0);
    // timestamp where node 52 visible is true
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2014-02-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    assertTrue(resultAfter instanceof Polygon);
    assertTrue(resultAfter.isValid());
    assertEquals(5, resultAfter.getCoordinates().length);
    Geometry expectedPolygonAfter = (new WKTReader()).read(
        "MULTIPOLYGON(((7.303 1.042, 7.31 1.06, 7.32 1.07, 7.32 1.04, 7.303 1.042)))"
    );
    Geometry intersectionAfter = resultAfter.intersection(expectedPolygonAfter);
    assertEquals(expectedPolygonAfter.getArea(), intersectionAfter.getArea(), DELTA);
  }

  @Test
  public void testTagChangeOfNodeInWay() throws ParseException {
    // relation, way member: node 53 changes tags, 51 changes coordinates
    OSMEntity entity = testData.relations().get(510L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.303 1.042,1.43 1.24,7.32 1.04,7.303 1.042)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // version after
    OSMEntity entityAfter = testData.relations().get(510L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2014-02-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    assertTrue(resultAfter instanceof Polygon);
    assertTrue(resultAfter.isValid());
    assertEquals(4, resultAfter.getCoordinates().length);
    Geometry expectedPolygonAfter = (new WKTReader()).read(
        "MULTIPOLYGON(((7.295 1.039, 1.43 1.24, 7.32 1.04, 7.295 1.039)))"
    );
    Geometry intersectionAfter = resultAfter.intersection(expectedPolygonAfter);
    assertEquals(expectedPolygonAfter.getArea(), intersectionAfter.getArea(), DELTA);
  }

  @Test
  public void testVisibleChangeOfWay() throws ParseException {
    // relation, way member: way 119 changes visible tag
    OSMEntity entity = testData.relations().get(511L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(4, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.29 1.01, 7.29 1.05, 7.30 1.01, 7.29 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // version after, visible false
    OSMEntity entityAfter = testData.relations().get(511L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2017-02-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    assertTrue(resultAfter.isEmpty());
  }

  @Test
  public void testVisibleChangeOfOneWayOfOuterRing() throws ParseException {
    // relation, 2 way members making outer ring: way 120 changes visible tag later, 121 not
    OSMEntity entity = testData.relations().get(512L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.5 1.04, 7.5 1.6, 7.4 1.6, 7.4 1.04,7.5 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // version after: way 120 does not exit any more
    OSMEntity entityAfter = testData.relations().get(512L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2018-02-01T00:00:00Z");
    Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
        areaDecider);
    assertTrue(resultAfter instanceof GeometryCollection);
    assertEquals(2, resultAfter.getNumGeometries());
  }

  @Test
  public void testTagChangeOfWay() throws ParseException {
    // relation, way member: way 122 changes tags
    OSMEntity entity = testData.relations().get(513L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01, 7.34 1.05, 7.32 1.05, 7.32 1.04,7.34 1.01)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // way first version
    OSMEntity entity1 = testData.relations().get(513L).get(0);
    OSHDBTimestamp timestamp1 =  TimestampParser.toOSHDBTimestamp("2009-02-01T00:00:00Z");
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertTrue(result1.isValid());
    assertEquals(5, result1.getCoordinates().length);
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01, 7.34 1.05, 7.32 1.05, 7.32 1.04,7.34 1.01)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
    // way second version
    OSMEntity entity2 = testData.relations().get(513L).get(0);
    OSHDBTimestamp timestamp2 =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Polygon);
    assertTrue(result2.isValid());
    assertEquals(5, result2.getCoordinates().length);
    Geometry expectedPolygon2 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.01, 7.34 1.05, 7.32 1.05, 7.32 1.04,7.34 1.01)))"
    );
    Geometry intersection2 = result2.intersection(expectedPolygon2);
    assertEquals(expectedPolygon2.getArea(), intersection2.getArea(), DELTA);
  }

  @Test
  public void testOneOfTwoPolygonDisappears() throws ParseException {
    // relation getting more ways, one disappears, last version not valid
    OSMEntity entity = testData.relations().get(514L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(9, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(514L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertTrue(result1.isValid());
    assertEquals(5, result1.getCoordinates().length);
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
  }

  @Test
  public void testWaySplitUpInTwo() throws ParseException {
    // relation, at the beginning one way, split up later into 2 ways
    OSMEntity entity = testData.relations().get(515L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Polygon);
    assertTrue(result.isValid());
    assertEquals(5, result.getCoordinates().length);
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.0 1.04, 7.0 1.6, 7.2 1.6, 7.2 1.04,7.0 1.04)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(515L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof Polygon);
    assertTrue(result1.isValid());
    assertEquals(5, result1.getCoordinates().length);
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON((( 7.0 1.04, 7.0 1.6, 7.2 1.6, 7.2 1.04,7.0 1.04)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
  }

  @Test
  public void testNullRefEntities() {
    // broken rel references (=invalid OSM data) can occur after "partial" data redactions
    OSMRelation rel = testData.relations().get(524L).get(0);
    OSHDBTimestamp timestamp = rel.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(rel, timestamp, areaDecider);
    // no exception should have been thrown at this point
    assertTrue(result.getNumGeometries() < rel.getMembers().length);
  }
}
