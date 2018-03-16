/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.celliterator.LazyEvaluatedContributionTypes;
import org.junit.Test;

/**
 *
 */
public class TestOSMContributionGetContributorUserId {
  public TestOSMContributionGetContributorUserId() throws Exception {
  }

  private final OSHEntity dummyOshEntity = OSHNode.build(Collections.singletonList(
      new OSMNode(-1L, 1, new OSHDBTimestamp(0L), 1L, 1, new int[]{}, 0, 0)
  ));

  @Test
  public void node() throws Exception {
    // timestamp match
    OSMContribution c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 1, new OSHDBTimestamp(123L), 1L, 7, new int[]{}, 0, 0), null,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
    // contribution type match
    c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 1, new OSHDBTimestamp(122L), 1L, 7, new int[] {}, 0, 0), null,
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.CREATION)),
        1L
    ));
    assertEquals(7, c.getContributorUserId());
    c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 2, new OSHDBTimestamp(122L), 2L, 7, new int[] {3,4}, 0, 0),
        new OSMNode(1L, 1, new OSHDBTimestamp(121L), 1L, 6, new int[] {1,2}, 0, 0),
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.TAG_CHANGE)),
        2L
    ));
    assertEquals(7, c.getContributorUserId());
    c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, -2, new OSHDBTimestamp(122L), 2L, 7, new int[] {}, 0, 0), // negative version == isVisible = false
        new OSMNode(1L, 1, new OSHDBTimestamp(121L), 1L, 6, new int[] {}, 0, 0),
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.of(ContributionType.DELETION)),
        2L
    ));
    // non-match
    assertEquals(7, c.getContributorUserId());
    c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMNode(1L, 1, new OSHDBTimestamp(122L), 1L, 7, new int[] {}, 0, 0),
        new OSMNode(1L, 1, new OSHDBTimestamp(122L), 1L, 7, new int[] {}, 0, 0),
        dummyOshEntity,
        null, null, null, null,
        new LazyEvaluatedContributionTypes(EnumSet.noneOf(ContributionType.class)),
        -1L
    ));
    assertEquals(-1, c.getContributorUserId());
  }

  @Test
  public void wayDirect() throws Exception {
    OSMContribution c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMWay(1L, 1, new OSHDBTimestamp(123L), 1L, 7, new int[] {}, new OSMMember[] {}), null,
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
    versions.add(new OSMNode(3L, 3, new OSHDBTimestamp(125L), 4L, 8, new int[] {}, 0, 0));
    versions.add(new OSMNode(3L, 2, new OSHDBTimestamp(123L), 3L, 7, new int[] {}, 0, 0));
    versions.add(new OSMNode(3L, 1, new OSHDBTimestamp(121L), 2L, 6, new int[] {}, 0, 0));

    OSMWay entity = new OSMWay(1L, 1, new OSHDBTimestamp(122L), 1L, 1, new int[] {}, new OSMMember[] {
        new OSMMember(3, OSMType.NODE, 0, OSHNode.build(versions))
    });
    OSMContribution c = new OSMContribution(new IterateAllEntry(
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
    OSMContribution c = new OSMContribution(new IterateAllEntry(
        new OSHDBTimestamp(123),
        new OSMRelation(1L, 1, new OSHDBTimestamp(123L), 1L, 7, new int[] {}, new OSMMember[] {}), null,
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
    versions.add(new OSMWay(3L, 3, new OSHDBTimestamp(125L), 4L, 8, new int[] {}, new OSMMember[] {}));
    versions.add(new OSMWay(3L, 2, new OSHDBTimestamp(123L), 3L, 7, new int[] {}, new OSMMember[] {}));
    versions.add(new OSMWay(3L, 1, new OSHDBTimestamp(121L), 2L, 6, new int[] {}, new OSMMember[] {}));

    OSMRelation entity = new OSMRelation(1L, 1, new OSHDBTimestamp(122L), 1L, 1, new int[] {}, new OSMMember[] {
        new OSMMember(3, OSMType.WAY, 0, OSHWay.build(versions, Collections.emptyList()))
    });
    OSMContribution c = new OSMContribution(new IterateAllEntry(
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
    nodeVersions.add(new OSMNode(3L, 3, new OSHDBTimestamp(125L), 4L, 8, new int[] {}, 0, 0));
    nodeVersions.add(new OSMNode(3L, 2, new OSHDBTimestamp(123L), 3L, 7, new int[] {}, 0, 0));
    nodeVersions.add(new OSMNode(3L, 1, new OSHDBTimestamp(121L), 2L, 6, new int[] {}, 0, 0));

    List<OSMWay> versions = new ArrayList<>();
    versions.add(new OSMWay(2L, 1, new OSHDBTimestamp(120L), 0L, 2, new int[] {}, new OSMMember[] {
        new OSMMember(3, OSMType.NODE, 0, OSHNode.build(nodeVersions))
    }));

    OSMRelation entity = new OSMRelation(1L, 1, new OSHDBTimestamp(110L), 1L, 1, new int[] {}, new OSMMember[] {
        new OSMMember(2, OSMType.WAY, 0, OSHWay.build(versions, Collections.singletonList(OSHNode.build(nodeVersions))))
    });
    OSMContribution c = new OSMContribution(new IterateAllEntry(
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
