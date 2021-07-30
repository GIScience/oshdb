package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests the parsing of filters and the application to OSM entity snapshots.
 */
public class ApplyOSMContributionTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();
  private final Geometry point = gf.createPoint();
  private final Geometry line = gf.createLineString();
  private final OSMEntity entity = createTestOSMEntityNode();
  private static final Set<ContributionType> NO_TYPE = EnumSet.noneOf(ContributionType.class);

  private static class TestOSMContribution implements OSMContribution {
    private static final String UNSUPPORTED_IN_TEST = "not supported for TestOSMEntitySnapshot";
    private final OSMEntity entityBefore;
    private final Geometry geometryBefore;
    private final OSMEntity entityAfter;
    private final Geometry geometryAfter;
    private final long changsetId;
    private final int contributorUserId;
    private final Set<ContributionType> contributionTypes;

    public TestOSMContribution(
        OSMEntity entityBefore, Geometry geometryBefore,
        OSMEntity entityAfter, Geometry geometryAfter,
        long changesetId, int contributorUserId, Set<ContributionType> contributionTypes) {
      this.entityBefore = entityBefore;
      this.geometryBefore = geometryBefore;
      this.entityAfter = entityAfter;
      this.geometryAfter = geometryAfter;
      this.changsetId = changesetId;
      this.contributorUserId = contributorUserId;
      this.contributionTypes = contributionTypes;
    }

    @Override
    public OSHDBTimestamp getTimestamp() {
      throw new UnsupportedOperationException(UNSUPPORTED_IN_TEST);
    }

    @Override
    public Geometry getGeometryBefore() {
      return this.geometryBefore;
    }

    @Override
    public Geometry getGeometryUnclippedBefore() {
      return this.geometryBefore;
    }

    @Override
    public Geometry getGeometryAfter() {
      return this.geometryAfter;
    }

    @Override
    public Geometry getGeometryUnclippedAfter() {
      return this.geometryAfter;
    }

    @Override
    public OSMEntity getEntityBefore() {
      return this.entityBefore;
    }

    @Override
    public OSMEntity getEntityAfter() {
      return this.entityAfter;
    }

    @Override
    public OSHEntity getOSHEntity() {
      throw new UnsupportedOperationException(UNSUPPORTED_IN_TEST);
    }

    @Override
    public boolean is(ContributionType contributionType) {
      return this.contributionTypes.contains(contributionType);
    }

    @Override
    public EnumSet<ContributionType> getContributionTypes() {
      return EnumSet.copyOf(this.contributionTypes);
    }

    @Override
    public int getContributorUserId() {
      return this.contributorUserId;
    }

    @Override
    public long getChangesetId() {
      return this.changsetId;
    }

    @Override
    public int compareTo(@NotNull OSMContribution contribution) {
      throw new UnsupportedOperationException(UNSUPPORTED_IN_TEST);
    }
  }

  @Test
  public void testBasicFallback() {
    FilterExpression expression = parser.parse("geometry:point");
    // expect true if either the "before" state …
    assertTrue(expression.applyOSMContribution(new TestOSMContribution(
        entity, point, entity, line, 1L, 2, NO_TYPE
    )));
    //  … or the "after" state matches the filter
    assertTrue(expression.applyOSMContribution(new TestOSMContribution(
        entity, line, entity, point, 1L, 2, NO_TYPE
    )));
    // neither the before nor after geometries match the filter
    assertFalse(expression.applyOSMContribution(new TestOSMContribution(
        entity, line, entity, line, 1L, 2, NO_TYPE
    )));
  }

  @Test
  public void testNegatableFilter() {
    FilterExpression expression = parser.parse("id:(1,2)");
    var testObject = new TestOSMContribution(
        entity, point, entity, point, 1L, 2, NO_TYPE
    );
    assertTrue(expression.applyOSMContribution(testObject));
    assertFalse(expression.negate().applyOSMContribution(testObject));
  }

  private void testContribution(FilterExpression expression) {
    assertFalse(expression.applyOSMContribution(new TestOSMContribution(
        entity, point, entity, point, 1L, 10, NO_TYPE
    )));
    assertTrue(expression.applyOSMContribution(new TestOSMContribution(
        entity, point, entity, point, 42L, 1, NO_TYPE
    )));
  }

  @Test
  public void testChangesetId() {
    testContribution(parser.parse("changeset:42"));
  }

  @Test
  public void testChangesetIdList() {
    testContribution(parser.parse("changeset:(41,42,43)"));
  }

  @Test
  public void testChangesetIdRange() {
    testContribution(parser.parse("changeset:(41..43)"));
  }

  @Test
  public void testContributorUserId() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    testContribution(parser.parse("contributor:1"));
  }

  @Test
  public void testContributorUserIdList() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    testContribution(parser.parse("contributor:(1,2,3)"));
  }

  @Test
  public void testContributorUserIdRange() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    testContribution(parser.parse("contributor:(1..2)"));
  }

  @Test
  public void testAndOperator() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    testContribution(parser.parse("contributor:1 and type:node"));
  }

  @Test
  public void testOrOperator() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    testContribution(parser.parse("contributor:1 or foo=doesntexist"));
  }
}
