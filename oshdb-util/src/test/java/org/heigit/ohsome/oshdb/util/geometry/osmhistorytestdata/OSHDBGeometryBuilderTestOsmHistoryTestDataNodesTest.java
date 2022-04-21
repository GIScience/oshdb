package org.heigit.ohsome.oshdb.util.geometry.osmhistorytestdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryTest;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * Tests the {@link OSHDBGeometryBuilder} class on OSM nodes.
 */
class OSHDBGeometryBuilderTestOsmHistoryTestDataNodesTest extends OSHDBGeometryTest {
  private static final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataNodesTest() {
    super("./src/test/resources/different-timestamps/node.osm");
  }

  @Test
  void testGeometryChange() {
    // A single node, lat lon changed over time

    // first appearance
    Geometry result = buildGeometry(nodes(1L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.22, ((Point) result).getY(), DELTA);

    // second
    result = buildGeometry(nodes(1L, 1));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.225, ((Point) result).getY(), DELTA);

    // last
    result = buildGeometry(nodes(1L, 2));
    assertTrue(result instanceof Point);
    assertEquals(1.425, ((Point) result).getX(), DELTA);
    assertEquals(1.23, ((Point) result).getY(), DELTA);

    // timestamp after newest timestamp
    result = buildGeometry(nodes(1L, 2), "2012-01-01T00:00:00Z");
    assertTrue(result instanceof Point);
    assertEquals(1.425, ((Point) result).getX(), DELTA);
    assertEquals(1.23, ((Point) result).getY(), DELTA);
  }

  @Test()
  void testInvalidAccess() {
    // A single node, lat lon changed over time
    OSMEntity entity = testData.nodes().get(1L).get(0);
    // timestamp before oldest timestamp
    OSHDBTimestamp timestampBefore =  TimestampParser.toOSHDBTimestamp("2007-01-01T00:00:00Z");
    assertThrows(AssertionError.class, () -> {
      OSHDBGeometryBuilder.getGeometry(entity, timestampBefore, areaDecider);
    });
  }

  @Test
  void testTagChange() {
    // A single node, tags changed over time
    Geometry result = buildGeometry(nodes(2L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(2L, 1));
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(2L, 2));
    assertTrue(result instanceof Point);
    assertEquals(1.43, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);
  }

  @Test
  void testVisibleChange() {
    // A single node, visible changes
    OSMEntity entity = testData.nodes().get(3L).get(0);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(entity);
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof Point);
    assertEquals(1.44, ((Point) result).getX(), DELTA);
    assertEquals(1.24, ((Point) result).getY(), DELTA);
    OSMEntity entity2 = testData.nodes().get(3L).get(1);
    OSHDBTimestamp timestamp2 = new OSHDBTimestamp(entity2);
    Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity2, timestamp2, areaDecider);
    assertTrue(result2 instanceof Point);
    assertTrue(result2.isEmpty());
    OSMEntity entity3 = testData.nodes().get(3L).get(2);
    OSHDBTimestamp timestamp3 = new OSHDBTimestamp(entity3);
    Geometry result3 = OSHDBGeometryBuilder.getGeometry(entity3, timestamp3, areaDecider);
    assertTrue(result3 instanceof Point);
    assertEquals(1.44, ((Point) result3).getX(), DELTA);
    assertEquals(1.24, ((Point) result3).getY(), DELTA);
    OSMEntity entity4 = testData.nodes().get(3L).get(3);
    OSHDBTimestamp timestamp4 = new OSHDBTimestamp(entity4);
    Geometry result4 = OSHDBGeometryBuilder.getGeometry(entity4, timestamp4, areaDecider);
    assertTrue(result4 instanceof Point);
    assertTrue(result4.isEmpty());
    OSMEntity entity5 = testData.nodes().get(3L).get(4);
    OSHDBTimestamp timestamp5 = new OSHDBTimestamp(entity5);
    Geometry result5 = OSHDBGeometryBuilder.getGeometry(entity5, timestamp5, areaDecider);
    assertTrue(result5 instanceof Point);
    assertEquals(1.44, ((Point) result5).getX(), DELTA);
    assertEquals(1.24, ((Point) result5).getY(), DELTA);
  }

  @Test
  void testMultipleChanges() {
    // A single node, various changes over time
    Geometry result = buildGeometry(nodes(4L, 0));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.21, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 1));
    assertTrue(result instanceof Point);
    assertEquals(1.425, ((Point) result).getX(), DELTA);
    assertEquals(1.20, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 2));
    assertTrue(result instanceof Point);
    assertTrue(result.isEmpty());

    result = buildGeometry(nodes(4L, 3));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.21, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 4));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.215, ((Point) result).getY(), DELTA);

    result = buildGeometry(nodes(4L, 5));
    assertTrue(result instanceof Point);
    assertEquals(1.42, ((Point) result).getX(), DELTA);
    assertEquals(1.215, ((Point) result).getY(), DELTA);
  }
}
