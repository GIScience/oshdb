package org.heigit.ohsome.oshdb.util.geometry.osmtestdata;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * Tests the {@link OSHDBGeometryBuilder} class: attribute tests.
 *
 * @see <a href="https://github.com/osmcode/osm-testdata/tree/master/grid">osm-testdata</a>
 */
class OSHDBGeometryBuilderTestOsmTestData3xxTest extends OSHDBGeometryTest {
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");

  OSHDBGeometryBuilderTestOsmTestData3xxTest() {
    super("./src/test/resources/osm-testdata/all.osm");
  }

  private Geometry buildEntityGeometry(long id) {
    OSMEntity entity = nodes(id, 0);
    return OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
  }

  @Test
  void test300() {
    // Normal node with uid (and user name)
    Geometry result = buildEntityGeometry(200000L);
    assertTrue(result instanceof Point);
    int entityUid = nodes(200000L, 0).getUserId();
    assertEquals(1, entityUid);
  }

  @Test()
  void test301() {
    assertDoesNotThrow(() -> {
      // Empty username on node should not happen
      buildEntityGeometry(201000L);
    });
  }

  @Test
  void test302() {
    // No uid and no user name means user is anonymous
    // user name is not priority
    int entityUid = nodes(202000L, 0).getUserId();
    assertTrue(entityUid < 1);
  }

  @Test
  void test303() {
    // uid 0 is the anonymous user
    int entityUid = nodes(203000L, 0).getUserId();
    assertEquals(0, entityUid);
  }

  @Test()
  void test304() {
    assertDoesNotThrow(() -> {
      // negative user ids are not allowed (but -1 could have been meant as anonymous user)
      buildEntityGeometry(204000L);
    });
  }

  @Test()
  void test305() {
    assertDoesNotThrow(() -> {
      // uid < 0 and username is inconsistent and definitely wrong
      buildEntityGeometry(205000L);
    });
  }

  @Test()
  void test306() {
    assertDoesNotThrow(() -> {
      // 250 characters in username is okay
      // user name is not priority
      buildEntityGeometry(206000L);
    });
  }

  @Test()
  void test307() {
    assertDoesNotThrow(() -> {
      // 260 characters in username is too long
      buildEntityGeometry(207000L);
    });
  }
}
