package org.heigit.ohsome.oshdb.contribution;

import static java.util.Collections.emptyList;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.CREATION;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.DELETION;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.GEOMETRY_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.MEMBER_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.TAG_CHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTest;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.junit.jupiter.api.Test;

class ContributionsWayTest extends OSHDBTest {

  @Test
  void testSingleVersionVisible() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(0), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());
    var members = contrib.getMembers();
    var member = members.get(0);
    assertEquals(OSMType.NODE, member.getType());
    assertEquals(1, member.getId());
  }

  @Test
  void testSingleVersionDeleted() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(-1, 1000, 101, 1, tags(), mems()));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(0), contrib);
    assertEquals(EnumSet.of(DELETION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testDeletedCreated() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(-2, 2000, 202, 2, tags(), mems()),
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(0), contrib);
    assertEquals(EnumSet.of(DELETION), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(1), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testFilterFailComplete() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(2, 2000, 202, 2, tags(), mems(1)),
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, x -> false);

    assertFalse(contribs.hasNext());
  }

  @Test
  void testFilterFailOnceInBetween() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(3, 3000, 303, 3, tags(), mems(1)),
        way(2, 2000, 202, 2, tags(), mems(1)),
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, x -> x.getUserId() != 2);

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
  void testFilterFailTwiceInBetween() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(4, 4000, 404, 4, tags(), mems(1)),
        way(3, 3000, 302, 2, tags(), mems(1)),
        way(2, 2000, 202, 2, tags(), mems(1)),
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, x -> x.getUserId() != 2);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(4000, 404, 4, versions.get(0), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    // version 3 should be missing

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(2), contrib);
    assertEquals(EnumSet.of(DELETION), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(3), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testFilterFailAtBeginning() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var versions = ways(1,
        way(3, 3000, 302, 2, tags(), mems(1)),
        way(2, 2000, 202, 2, tags(), mems(1)),
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, x -> x.getUserId() != 2);

    // version 3 should be missing

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(1), contrib);
    assertEquals(EnumSet.of(DELETION), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(2), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testSquashMembers() {
    var nodes = List.of(
        osh(1,
            node(3, 2500, 250, 3, tags(), 0, 0), // squash
            node(2, 1500, 200, 2, tags(), 0, 0), // squash major
            node(1, 1000, 100, 1, tags(), 0, 0)),
        osh(2,
            node(3, 3500, 250, 3, tags(), 1, 1),
            node(2, 2500, 250, 3, tags(), 1, 1), // squash
            node(1, 2000, 200, 2, tags(), 0, 0)));

    var versions = ways(1,
        way(2, 2000, 200, 2, tags(tag(1, 1)), mems(1, 2)),
        way(1, 1000, 100, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(3500, 250, 3, versions.get(0), contrib);
    assertEquals(EnumSet.of(GEOMETRY_CHANGE), contrib.getMinorTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 200, 2, versions.get(0), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE, TAG_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 100, 1, versions.get(1), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testMemberChanges() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(2,
            node(1, 2000, 202, 2, tags(), 0, 0)),
        osh(3,
            node(1, 2000, 202, 2, tags(), 0, 0))
        );

    var versions = ways(1,
        way(5, 5000, 505, 5, tags(), mems(3)), // del member
        way(4, 4000, 404, 4, tags(), mems(1, 3)), // no change
        way(3, 3000, 303, 3, tags(), mems(1, 3)), // member change
        way(2, 2000, 202, 2, tags(), mems(1, 2)), // new member
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(5000, 505, 5, versions.get(0), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(4000, 404, 4, versions.get(1), contrib);
    assertEquals(EnumSet.noneOf(ContributionType.class), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(3000, 303, 3, versions.get(2), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(3), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(4), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNoMembers() {
    var versions = ways(1,
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, emptyList());
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(0), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testMinorContribOutsideMax() {
    var nodes = List.of(
        osh(1,
            node(2, 2000, 202, 2, tags(), 0, 0),
            node(1, 1000, 101, 1, tags(), 0, 0)));

    var versions = ways(1,
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh, 1500);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(0), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNewMemberContribOutside() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(2,
            node(2, 3000, 303, 3, tags(), 0, 0),
            node(1, 1000, 101, 1, tags(), 0, 0)));

    var versions = ways(1,
        way(2, 2000, 202, 2, tags(), mems(1)),
        way(1, 1000, 101, 1, tags(), mems(2)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(0), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(1), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNewMemberNoContribInside() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(2,
            node(1, 3000, 303, 3, tags(), 0, 0)));

    var versions = ways(1,
        way(2, 2000, 202, 2, tags(), mems(1)),
        way(1, 1000, 101, 1, tags(), mems(2)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(0), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(1), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testDifferentMember() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(2,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(3,
            node(1, 1000, 101, 1, tags(), 0, 0)));

    var versions = ways(1,
        way(2, 2000, 202, 2, tags(), mems(2, 3, 2)),
        way(1, 1000, 101, 1, tags(), mems(1, 2)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(0), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(1), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testEmptyMemberContribs() {
    var nodes = List.of(
        osh(1,
            node(1, 3000, 303, 3, tags(), 0, 0)),
        osh(2,
            node(1, 2000, 202, 2, tags(), 0, 0)));

    var versions = ways(1,
        way(3, 3000, 303, 3, tags(), mems(1, 2)),
        way(2, 2000, 202, 2, tags(), mems(2)),
        way(1, 1000, 101, 1, tags(), mems(1)));
    var osh = osh(versions, nodes);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertContrib(3000, 303, 3, versions.get(0), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(2000, 202, 2, versions.get(1), contrib);
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertContrib(1000, 101, 1, versions.get(2), contrib);
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }
}
