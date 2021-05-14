package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygonal;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on OSM relations except multipolygon relations.
 */
public class OSHDBGeometryBuilderTestOsmHistoryTestDataRelationNotMultipolygonTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataRelationNotMultipolygonTest() {
    testData.add("./src/test/resources/different-timestamps/type-not-multipolygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void testGeometryChange() {
    // relation getting more ways, one disappears, last version not valid
    OSMEntity entity = testData.relations().get(500L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity1 = testData.relations().get(500L).get(1);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1 instanceof GeometryCollection);
      assertTrue(result1.isValid());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // third version
    OSMEntity entity2 = testData.relations().get(500L).get(2);
    OSHDBTimestamp timestamp2 = new OSHDBTimestamp(entity2);
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
  public void testVisibleChange() {
    // relation  visible tag changed
    OSMEntity entity = testData.relations().get(501L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(2, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity1 = testData.relations().get(501L).get(1);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // third version
    OSMEntity entity2 = testData.relations().get(501L).get(2);
    OSHDBTimestamp timestamp2 = new OSHDBTimestamp(entity2);
    try {
      Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
      assertTrue(result2 instanceof GeometryCollection);
      assertTrue(result2.isValid());
      assertEquals(2, result2.getNumGeometries());
      assertTrue(result2.getGeometryN(0) instanceof LineString);
      assertTrue(result2.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testWaysNotExistent() {
    // relation with three ways, all not existing
    OSMEntity entity = testData.relations().get(502L).get(0);
    Geometry result;
    try {
      OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
      result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertTrue(result.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTagChange() {
    // relation tags changing
    OSMEntity entity = testData.relations().get(503L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity1 = testData.relations().get(503L).get(1);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1 instanceof GeometryCollection);
      assertTrue(result1.isValid());
      assertEquals(1, result1.getNumGeometries());
      assertTrue(result1.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // third version
    OSMEntity entity2 = testData.relations().get(503L).get(2);
    OSHDBTimestamp timestamp2 = new OSHDBTimestamp(entity2);
    try {
      Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
      assertTrue(result2 instanceof GeometryCollection);
      assertTrue(result2.isValid());
      assertEquals(1, result2.getNumGeometries());
      assertTrue(result2.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testGeometryChangeOfNodeRefsInWays() {
    // relation, way 109 -inner- and 110 -outer- ways changed node refs
    OSMEntity entity = testData.relations().get(504L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(2, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity1 = testData.relations().get(504L).get(1);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1 instanceof GeometryCollection);
      assertTrue(result1.isValid());
      assertEquals(2, result1.getNumGeometries());
      assertTrue(result1.getGeometryN(0) instanceof LineString);
      assertTrue(result1.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version in between
    OSMEntity entityBetween = testData.relations().get(504L).get(0);
    OSHDBTimestamp timestampBetween =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    try {
      Geometry resultBetween = OSHDBGeometryBuilder
          .getGeometry(entityBetween, timestampBetween, areaDecider);
      assertTrue(resultBetween instanceof GeometryCollection);
      assertTrue(resultBetween.isValid());
      assertEquals(2, resultBetween.getNumGeometries());
      assertTrue(resultBetween.getGeometryN(0) instanceof LineString);
      assertTrue(resultBetween.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testGeometryChangeOfNodeCoordinatesInWay() {
    // relation, way 112  changed node coordinates
    OSMEntity entity = testData.relations().get(505L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version after
    OSMEntity entityAfter = testData.relations().get(505L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    try {
      Geometry resultAfter = OSHDBGeometryBuilder
          .getGeometry(entityAfter, timestampAfter, areaDecider);
      assertTrue(resultAfter instanceof GeometryCollection);
      assertTrue(resultAfter.isValid());
      assertEquals(1, resultAfter.getNumGeometries());
      assertTrue(resultAfter.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testGeometryChangeOfNodeCoordinatesInRelationAndWay() {
    // relation, with node members, nodes changed coordinates
    OSMEntity entity = testData.relations().get(506L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(3, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof Point);
      assertTrue(result.getGeometryN(1) instanceof Point);
      assertTrue(result.getGeometryN(2) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version after
    OSMEntity entityAfter = testData.relations().get(506L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    try {
      Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
          areaDecider);
      assertTrue(resultAfter instanceof GeometryCollection);
      assertTrue(resultAfter.isValid());
      assertEquals(3, resultAfter.getNumGeometries());
      assertTrue(resultAfter.getGeometryN(0) instanceof Point);
      assertTrue(resultAfter.getGeometryN(1) instanceof Point);
      assertTrue(resultAfter.getGeometryN(2) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testGeometryCollection() {
    // relation, not valid, should be a not empty geometryCollection
    OSMEntity entity = testData.relations().get(507L).get(0);
    try {
      OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
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
    Geometry result;
    try {
      OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
      result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testVisibleChangeOfNodeInWay() {
    // relation, way member: node 52 changes visible tag
    OSMEntity entity = testData.relations().get(509L).get(0);
    // timestamp where node 52 visible is false
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version after
    OSMEntity entityAfter = testData.relations().get(509L).get(0);
    // timestamp where node 52 visible is true
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2014-02-01T00:00:00Z");
    try {
      Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
          areaDecider);
      assertTrue(resultAfter instanceof GeometryCollection);
      assertTrue(resultAfter.isValid());
      assertEquals(1, resultAfter.getNumGeometries());
      assertTrue(resultAfter.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTagChangeOfNodeInWay() {
    // relation, way member: node 53 changes tags, 51 changes coordinates
    OSMEntity entity = testData.relations().get(510L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version after
    OSMEntity entityAfter = testData.relations().get(510L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2014-02-01T00:00:00Z");
    try {
      Geometry resultAfter = OSHDBGeometryBuilder
          .getGeometry(entityAfter, timestampAfter, areaDecider);
      assertTrue(resultAfter instanceof GeometryCollection);
      assertTrue(resultAfter.isValid());
      assertEquals(1, resultAfter.getNumGeometries());
      assertTrue(resultAfter.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testVisibleChangeOfWay() {
    // relation, way member: way 119 changes visible tag
    OSMEntity entity = testData.relations().get(511L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version after, visible false
    OSMEntity entityAfter = testData.relations().get(511L).get(0);
    OSHDBTimestamp timestampAfter =  TimestampParser.toOSHDBTimestamp("2017-02-01T00:00:00Z");
    try {
      Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
          areaDecider);
      assertTrue(resultAfter instanceof GeometryCollection);
      assertTrue(resultAfter.isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testVisibleChangeOfOneWayOfOuterRing() {
    // relation, 2 way members making outer ring: way 120 changes visible tag later, 121 not
    OSMEntity entity = testData.relations().get(512L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(2, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // version after: way 120 does not exit any more
    OSMEntity entityAfter = testData.relations().get(512L).get(0);
    OSHDBTimestamp timestampAfter = TimestampParser.toOSHDBTimestamp("2018-02-01T00:00:00Z");
    try {
      Geometry resultAfter = OSHDBGeometryBuilder.getGeometry(entityAfter, timestampAfter,
          areaDecider);
      assertTrue(resultAfter instanceof GeometryCollection);
      assertEquals(2, resultAfter.getNumGeometries());
      assertTrue(resultAfter.getGeometryN(0) instanceof LineString
          || resultAfter.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTagChangeOfWay() {
    // relation, way member: way 122 changes tags
    OSMEntity entity = testData.relations().get(513L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // way first version
    OSMEntity entity1 = testData.relations().get(513L).get(0);
    OSHDBTimestamp timestamp1 =  TimestampParser.toOSHDBTimestamp("2009-02-01T00:00:00Z");
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1 instanceof GeometryCollection);
      assertTrue(result1.isValid());
      assertEquals(1, result1.getNumGeometries());
      assertTrue(result1.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // way second version
    OSMEntity entity2 = testData.relations().get(513L).get(0);
    OSHDBTimestamp timestamp2 =  TimestampParser.toOSHDBTimestamp("2012-02-01T00:00:00Z");
    try {
      Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
      assertTrue(result2 instanceof GeometryCollection);
      assertTrue(result2.isValid());
      assertEquals(1, result2.getNumGeometries());
      assertTrue(result2.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testOneOfTwoPolygonDisappears() {
    // relation, at the beginning two polygons, one disappears later
    OSMEntity entity = testData.relations().get(514L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(2, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity1 = testData.relations().get(514L).get(1);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1 instanceof GeometryCollection);
      assertTrue(result1.isValid());
      assertEquals(1, result1.getNumGeometries());
      assertTrue(result1.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testWaySplitUpInTwo() {
    // relation, at the beginning one way, split up later into 2 ways
    OSMEntity entity = testData.relations().get(515L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertTrue(result.isValid());
      assertEquals(1, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity1 = testData.relations().get(515L).get(1);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result1 instanceof GeometryCollection);
      assertTrue(result1.isValid());
      assertEquals(2, result1.getNumGeometries());
      assertTrue(result1.getGeometryN(0) instanceof LineString);
      assertTrue(result1.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testRestrictionRoles() {
    // relation, restriction, role changes
    OSMEntity entity1 = testData.relations().get(518L).get(0);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertEquals(3, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof Point);
      assertTrue(result.getGeometryN(2) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testRolesArePartAndOutline() {
    // relation as building with role=part and outline
    OSMEntity entity1 = testData.relations().get(519L).get(0);
    OSHDBTimestamp timestamp1 = new OSHDBTimestamp(entity1);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
      assertTrue(result instanceof GeometryCollection);
      assertEquals(2, result.getNumGeometries());
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    // second version
    OSMEntity entity2 = testData.relations().get(519L).get(1);
    OSHDBTimestamp timestamp2 = new OSHDBTimestamp(entity2);
    try {
      Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
      assertTrue(result2 instanceof GeometryCollection);
      assertEquals(3, result2.getNumGeometries());
      assertTrue(result2.getGeometryN(0) instanceof LineString);
      assertTrue(result2.getGeometryN(1) instanceof LineString);
      assertTrue(result2.getGeometryN(2) instanceof LineString);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

}

