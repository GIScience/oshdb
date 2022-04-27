package org.heigit.ohsome.oshdb.contribution;

import static org.heigit.ohsome.oshdb.contribution.ContributionType.GEOMETRY_CHANGE;
import static org.heigit.ohsome.oshdb.contribution.ContributionType.MEMBER_CHANGE;
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
  void test() {
    var nodes = List.of(
        osh(1,
            node(1, 1000, 101, 1, tags(), 0, 0)));
    var ways = List.of(
        osh(1, List.of(
            osh(1,
                node(3, 3500, 253, 3, tags(), 1, 1),
                node(2, 2500, 253, 3, tags(), 1, 1), // squash
                node(1, 2000, 202, 2, tags(), 0, 0))),
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
    assertEquals(1000, contrib.getEpochSecond());
    assertEquals(101, contrib.getChangeset());
    assertEquals(1, contrib.getUser());
    assertEquals(versions.get(1), contrib.getEntity());
    assertEquals(EnumSet.of(ContributionType.CREATION), contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

}
