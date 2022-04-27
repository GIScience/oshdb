package org.heigit.ohsome.oshdb.contribution;

import static org.junit.jupiter.api.Assertions.*;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBTestHelper;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.junit.jupiter.api.Test;

class ContributionsNodeTest {
  public static final Set<ContributionType> CREATION = EnumSet.of(ContributionType.CREATION);

  @Test
  void testSingleVersion() {
    var versions = List.of(
        OSM.node(1, 1, 1000, 100, 1, OSHDBTestHelper.tags(), 0, 0));
    OSHNode osh = OSHDBTestHelper.oshNode(versions);
    var contribs = Contributions.of(osh);

    assertTrue(contribs.hasNext());
    var contrib = contribs.next();
    assertEquals(contrib.getEpochSecond(), 1000);
    assertEquals(contrib.getChangeset(), 100);
    assertEquals(contrib.getUser(), 1);
    assertEquals(versions.get(0), contrib.getEntity());
    assertEquals(CREATION, contrib.getTypes());

    assertFalse(contribs.hasNext());
  }

  @Test
  void testSingleVersionAfterMaxTimestamp() {
    var versions = List.of(OSM.node(1, 1, 1000, 100, 1, OSHDBTestHelper.tags(), 0, 0));
    OSHNode osh = OSHDBTestHelper.oshNode(versions);
    var contribs = Contributions.of(osh, 500);

    assertFalse(contribs.hasNext());
  }

  @Test
  void testSingleVersionFilterFail() {
    var versions = List.of(OSM.node(1, 1, 1000, 100, 1, OSHDBTestHelper.tags(), 0, 0));
    OSHNode osh = OSHDBTestHelper.oshNode(versions);
    var contribs = Contributions.of(osh, Long.MAX_VALUE, o -> false);

    assertFalse(contribs.hasNext());
  }

}
