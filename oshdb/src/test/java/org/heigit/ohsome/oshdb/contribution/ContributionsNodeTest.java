package org.heigit.ohsome.oshdb.contribution;

import static org.heigit.ohsome.oshdb.contribution.ContributionType.CREATION;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.DELETION;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.GEOMETRY_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.TAG_CHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import org.heigit.ohsome.oshdb.OSHDBTest;
import org.junit.jupiter.api.Test;

class ContributionsNodeTest extends OSHDBTest {

  @Test
  void testReviveDeletedCreated() {
    var versions = nodes(1,
        node(3, 3000, 303, 3, tags(tag(1, 1)), 1, 0), //revive
        node(-2, 2000, 202, 2, tags(), 0, 0), // deleted
        node(1, 1000, 101, 1, tags(tag(1, 1)), 1, 0)); //created
    var osh = osh(versions);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(3000, 303, 3, versions.get(0), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(1), contrib);
    assertEquals(EnumSet.of(DELETION), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(2), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testGeomChange() {
    var versions = nodes(1,
        node(3, 3000, 303, 3, tags(), 1, 0),
        node(2, 2000, 202, 2, tags(), 0, 1),
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(3000, 303, 3, versions.get(0), contrib);
    assertEquals(EnumSet.of(GEOMETRY_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(1), contrib);
    assertEquals(EnumSet.of(GEOMETRY_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(2), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testGeomTagCreation() {
    var versions = nodes(1,
        node(3, 3000, 303, 3, tags(tag(1, 1)), 1, 0), // geom-change
        node(2, 2000, 202, 2, tags(tag(1, 1)), 0, 0), // tag-change
        node(1, 1000, 101, 1, tags(), 0, 0)); // creation
    var osh = osh(versions);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(3000, 303, 3, versions.get(0), contrib);
    assertEquals(EnumSet.of(GEOMETRY_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(1), contrib);
    assertEquals(EnumSet.of(TAG_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(2), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testSingleVersionAfterMaxTimestamp() {
    var versions = nodes(1,
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh, 500);

    assertFalse(contribs.hasNext());
  }

  @Test
  void testSingleVersionFilterFail() {
    var versions = nodes(1,
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, o -> false);

    assertFalse(contribs.hasNext());
  }

  @Test
  void testMultipleVersionFilterFail() {
    var versions = nodes(1,
        node(2, 2000, 202, 2, tags(), 0, 0),
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, o -> false);

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNoVersionBeforeMax() {
    var versions = nodes(1,
        node(2, 2000, 202, 2, tags(), 0, 0),
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh, 500);

    assertFalse(contribs.hasNext());
  }

  @Test
  void testSquashed() {
    var versions = nodes(1,
        node(2, 2000, 101, 1, tags(), 0, 0),
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(2000, 101, 1, versions.get(0), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testCheckMembersEmpty() {
    var versions = nodes(1,
        node(1, 1000, 101, 1, tags(), 0, 0));
    var osh = osh(versions);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(0), contrib);

    assertFalse(contribs.hasNext());
  }
}
