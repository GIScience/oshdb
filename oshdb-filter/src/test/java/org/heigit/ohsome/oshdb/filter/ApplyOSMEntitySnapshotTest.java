package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests the parsing of filters and the application to OSM entity snapshots.
 */
public class ApplyOSMEntitySnapshotTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();

  private static class TestOSMEntitySnapshot implements OSMEntitySnapshot {
    private static final String UNSUPPORTED_IN_TEST = "not supported for TestOSMEntitySnapshot";
    private final OSMEntity entity;
    private final Geometry geometry;

    public TestOSMEntitySnapshot(OSMEntity entity, Geometry geometry) {
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
  public void testBasicFallback() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSMEntitySnapshot(new TestOSMEntitySnapshot(
        createTestOSMEntityNode(), gf.createPoint())));
    assertFalse(expression.applyOSMEntitySnapshot(new TestOSMEntitySnapshot(
        createTestOSMEntityNode(), gf.createLineString())));
  }

  @Test(expected = IllegalStateException.class)
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void testChangesetId() {
    FilterExpression expression = parser.parse("changeset:42");
    expression.applyOSMEntitySnapshot(
        new TestOSMEntitySnapshot(createTestOSMEntityNode(), gf.createPoint()));
    fail();
  }

  @Test(expected = IllegalStateException.class)
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void testContributorUserId() {
    FilterExpression expression = (new FilterParser(tagTranslator, true)).parse("contributor:1");
    expression.applyOSMEntitySnapshot(
        new TestOSMEntitySnapshot(createTestOSMEntityNode(), gf.createPoint()));
    fail();
  }
}
