package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.object.OSMContributionImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.celliterator.LazyEvaluatedContributionTypes;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.junit.Test;

/**
 * Tests the get contributor user id method of the OSHDB API.
 */
public class TestOSMContributionGetContributorUserId {
  public TestOSMContributionGetContributorUserId() throws Exception {
  }

  private final OSHEntity dummyOshEntity = OSHNodeImpl.build(Collections.singletonList(
      new OSMNode(-1L, 1, 0L, 1L, 1, new int[]{}, 0, 0)
  ));

  @Test
  public void node() throws Exception {
    // timestamp match
    OSMContribution c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 1, 123L, 1L, 7, new int[]{}, 0, 0), null,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
    // contribution type match
    c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 1, 122L, 1L, 7, new int[] {}, 0, 0), null,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
    c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 2, 122L, 2L, 7, new int[] { 3, 4 }, 0, 0),
        new OSMNode(1L, 1, 121L, 1L, 6, new int[] { 1, 2 }, 0, 0),
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.TAG_CHANGE)),
        2L
    ));
    assertEquals(7, c.getContributorUserId());
    c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        // negative version == isVisible = false
        new OSMNode(1L, -2, 122L, 2L, 7, new int[] {}, 0, 0),
        new OSMNode(1L, 1, 121L, 1L, 6, new int[] {}, 0, 0),
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION)),
        2L
    ));
    // non-match
    assertEquals(7, c.getContributorUserId());
    c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 1, 122L, 1L, 7, new int[] {}, 0, 0),
        new OSMNode(1L, 1, 122L, 1L, 7, new int[] {}, 0, 0),
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.noneOf(ContributionType.class)),
        -1L
    ));
    assertEquals(-1, c.getContributorUserId());
  }

  @Test
  public void wayDirect() throws Exception {
    OSMContribution c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMWay(1L, 1, 123L, 1L, 7, new int[] {}, new OSMMember[] {}), null,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
  }

  @Test
  public void wayIndirect() throws Exception {
    List<OSMNode> versions = new ArrayList<>();
    versions.add(new OSMNode(3L, 3, 125L, 4L, 8, new int[] {}, 0, 0));
    versions.add(new OSMNode(3L, 2, 123L, 3L, 7, new int[] {}, 0, 0));
    versions.add(new OSMNode(3L, 1, 121L, 2L, 6, new int[] {}, 0, 0));

    OSMWay entity = new OSMWay(
        1L, 1, 122L, 1L, 1, new int[] {}, new OSMMember[] {
          new OSMMember(3, OSMType.NODE, 0, OSHNodeImpl.build(versions))
        });
    OSMContribution c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        entity, entity,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.GEOMETRY_CHANGE)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
  }

  @Test
  public void relationDirect() throws Exception {
    OSMContribution c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMRelation(1L, 1, 123L, 1L, 7, new int[] {}, new OSMMember[] {}),
        null,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
  }

  @Test
  public void relationIndirectWay() throws Exception {
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
        new OSMWay(3L, 3, 125L, 4L, 8, new int[] {}, new OSMMember[] {})
    );
    versions.add(
        new OSMWay(3L, 2, 123L, 3L, 7, new int[] {}, new OSMMember[] {})
    );
    versions.add(
        new OSMWay(3L, 1, 121L, 2L, 6, new int[] {}, new OSMMember[] {})
    );

    OSMRelation entity = new OSMRelation(
        1L, 1, 122L, 1L, 1, new int[] {}, new OSMMember[] {
          new OSMMember(3, OSMType.WAY, 0, OSHWayImpl.build(versions, Collections.emptyList()))
        });
    OSMContribution c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        entity, entity,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.GEOMETRY_CHANGE)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
  }

  @Test
  public void relationIndirectWayNode() throws Exception {
    List<OSMNode> nodeVersions = new ArrayList<>();
    nodeVersions.add(new OSMNode(3L, 3, 125L, 4L, 8, new int[] {}, 0, 0));
    nodeVersions.add(new OSMNode(3L, 2, 123L, 3L, 7, new int[] {}, 0, 0));
    nodeVersions.add(new OSMNode(3L, 1, 121L, 2L, 6, new int[] {}, 0, 0));

    List<OSMWay> versions = new ArrayList<>();
    versions.add(new OSMWay(2L, 1, 120L, 0L, 2, new int[] {}, new OSMMember[] {
        new OSMMember(3, OSMType.NODE, 0, OSHNodeImpl.build(nodeVersions))
    }));

    OSMRelation entity = new OSMRelation(
        1L, 1, 110L, 1L, 1, new int[] {}, new OSMMember[] {
          new OSMMember(
              2, OSMType.WAY, 0,
              OSHWayImpl.build(versions, Collections.singletonList(OSHNodeImpl.build(nodeVersions)))
          )
        });
    OSMContribution c = new OSMContributionImpl(new IterateAllEntry(
        new OSHDBTimestamp(123),
        entity, entity,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.GEOMETRY_CHANGE)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
  }


}
