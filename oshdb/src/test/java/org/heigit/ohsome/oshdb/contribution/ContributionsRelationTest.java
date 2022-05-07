package org.heigit.ohsome.oshdb.contribution;

import static java.util.Collections.emptyList;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.CREATION;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.DELETION;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.GEOMETRY_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.MEMBER_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.ROLE_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.TAG_CHANGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTest;
import org.junit.jupiter.api.Test;

class ContributionsRelationTest extends OSHDBTest {

  @Test
  void testMemberTagChanges() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(3, 3500, 253, 3, tags(), 1, 1),
                node(2, 2500, 253, 3, tags(), 1, 1), // squash
                node(1, 1100, 101, 1, tags(), 0, 0))), // minor-major change
            way(1, 1000, 101, 1, tags(), mems(1))));

    var versions = relations(1,
        relation(2, 2000, 202, 2, tags(tag(1, 1)), mems(w(1, 0))),
        relation(1, 1000, 101, 1, tags(), mems(w(1, 0), n(1, 0))));
    var osh = osh(versions, nodes, ways);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(3500, contrib.getEpochSecond());
    assertEquals(253, contrib.getChangeset());
    assertEquals(3, contrib.getUser());
    assertEquals(versions.get(0), contrib.getEntity());
    assertEquals(EnumSet.of(GEOMETRY_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(2000, contrib.getEpochSecond());
    assertEquals(202, contrib.getChangeset());
    assertEquals(2, contrib.getUser());
    assertEquals(versions.get(0), contrib.getEntity());
    assertEquals(EnumSet.of(TAG_CHANGE, MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(1100, contrib.getEpochSecond());
    assertEquals(101, contrib.getChangeset());
    assertEquals(1, contrib.getUser());
    assertEquals(versions.get(1), contrib.getEntity());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testDeletionCreation() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))));

    var versions = relations(1,
        relation(-2, 2000, 202, 2, tags(), mems(w(1, 0), n(1, 0))),
        relation(1, 1000, 101, 1, tags(), mems(w(1, 0), n(1, 0))));
    var osh = osh(versions, nodes, ways);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(2000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(DELETION), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testMemberChanges() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(2,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))));

    var versions = relations(1,
        relation(4, 4000, 404, 4, tags(), mems(n(2, 0), w(1, 0))), // id change
        relation(3, 3000, 303, 3, tags(), mems(n(1, 0), w(1, 0))), // type change
        relation(2, 2000, 202, 2, tags(), mems(w(1, 0), n(1, 1))), // role change
        relation(1, 1000, 101, 1, tags(), mems(w(1, 0), n(1, 0))));

    var osh = osh(versions, nodes, ways);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(4000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(3000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(2000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(ROLE_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNoNodes() {
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))));

    var versions = relations(1,
        relation(1, 1000, 101, 1, tags(), mems(w(1, 0))));

    var osh = osh(versions, emptyList(), ways);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNoWays() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));

    var versions = relations(1,
        relation(1, 1000, 101, 1, tags(), mems(n(1, 0))));

    var osh = osh(versions, nodes, emptyList());
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testNoMembers() {
    var versions = relations(1,
        relation(1, 1000, 101, 1, tags(), mems(n(1, 0))));

    var osh = osh(versions, emptyList(), emptyList());
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testMissingMemberChanges() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))));

    var versions = relations(1,
        relation(1, 1000, 101, 1, tags(), mems(w(1, 0), r(1, 0))));

    var osh = osh(versions, nodes, ways);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testDifferentWayAndNode() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(2,
            node(1, 1000, 101, 1, tags(), 0, 0)),
        osh(3,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))),
        osh(2, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))),
        osh(3, List.of(
            osh(1,
                node(1, 1000, 101, 1, tags(), 0, 0))),
            way(1, 1000, 101, 1, tags(), mems(1))));

    var versions = relations(1,
        relation(2, 2000, 202, 2, tags(), mems(w(1, 0), n(1, 0), w(2, 0), n(2, 0))),
        relation(1, 1000, 101, 1, tags(), mems(w(1, 0), n(1, 0), w(3, 0), n(3, 0))));

    var osh = osh(versions, nodes, ways);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(2000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(MEMBER_CHANGE), contrib.getTypes());

    assertTrue(contribs.hasNext());
    contrib = contribs.next();
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(EnumSet.of(CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }
}
