package org.heigit.ohsome.oshdb.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests the parsing of filters and the application to OSM entity snapshots.
 */
class ApplyOSMEntitySnapshotTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();

  private static class TestOSMEntitySnapshot implements OSMEntitySnapshot {
    private static final String UNSUPPORTED_IN_TEST = "not supported for TestOSMEntitySnapshot";
    private final OSMEntity entity;
    private final Geometry geometry;

    TestOSMEntitySnapshot(OSMEntity entity, Geometry geometry) {
      this.entity = entity;
      this.geometry = geometry;
    }

    @Override
    public OSHDBTimestamp getTimestamp() {
      throw new UnsupportedOperationException(UNSUPPORTED_IN_TEST);
    }

    @Override
    public Geometry getGeometry() {
      return this.geometry;
    }

    @Override
    public Geometry getGeometryUnclipped() {
      return this.geometry;
    }

    @Override
    public OSMEntity getEntity() {
      return this.entity;
    }

    @Override
    public OSHEntity getOSHEntity() {
      throw new UnsupportedOperationException(UNSUPPORTED_IN_TEST);
    }

    @Override
    public int compareTo(@NotNull OSMEntitySnapshot snapshot) {
      throw new UnsupportedOperationException(UNSUPPORTED_IN_TEST);
    }
  }

  @Test
  void testBasicFallback() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSMEntitySnapshot(new TestOSMEntitySnapshot(
        createTestOSMEntityNode(), gf.createPoint())));
    assertFalse(expression.applyOSMEntitySnapshot(new TestOSMEntitySnapshot(
        createTestOSMEntityNode(), gf.createLineString())));
    assertFalse(expression.negate().applyOSMEntitySnapshot(new TestOSMEntitySnapshot(
        createTestOSMEntityNode(), gf.createPoint())));
  }

  @Test
  void testNegatableFilter() {
    FilterExpression expression = parser.parse("id:(1,2)");
    var testObject = new TestOSMEntitySnapshot(createTestOSMEntityNode(), gf.createPoint());
    assertTrue(expression.applyOSMEntitySnapshot(testObject));
    assertFalse(expression.negate().applyOSMEntitySnapshot(testObject));
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void testFailWithSnapshot(FilterExpression expression) {
    expression.applyOSMEntitySnapshot(
        new TestOSMEntitySnapshot(createTestOSMEntityNode(), gf.createPoint()));
    fail();
  }

  @Test()
  void testChangesetId() {
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("changeset:42"));
    });
  }

  @Test()
  void testChangesetIdList() {
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("changeset:(41,42,43)"));
    });
  }

  @Test()
  void testChangesetIdRange() {
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("changeset:(41..43)"));
    });
  }

  @Test()
  void testContributorUserId() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("contributor:1"));
    });
  }

  @Test()
  void testContributorUserIdList() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("contributor:(1,2,3)"));
    });
  }

  @Test()
  void testContributorUserIdRange() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("contributor:(1..3)"));
    });
  }

  @Test()
  void testAndOperator() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("contributor:1 and type:node"));
    });
  }

  @Test()
  void testOrOperator() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    assertThrows(IllegalStateException.class, () -> {
      testFailWithSnapshot(parser.parse("contributor:1 or foo=doesntexist"));
    });
  }
}
