package org.heigit.ohsome.oshdb.util.osh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link OSHEntityTimeUtils} class.
 */
@SuppressWarnings("javadoc")
class TestOSHEntityTimeUtils {
  @Test
  void testGetModificationTimestampsNode() throws IOException {
    OSHNode hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 2, 2L, 0L, 1, new int[] {1, 1},
            86756350, 494186210),
        OSM.node(123L, 1, 1L, 0L, 1, new int[] {1, 1},
            86756350, 494186210)
    ));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hnode);
    assertNotNull(tss);
    assertEquals(2, tss.size());
    assertEquals(1L, tss.get(0).getEpochSecond());
    assertEquals(2L, tss.get(1).getEpochSecond());

    // additionally, also make sure that the same result is returned by the "recurse" variant
    assertEquals(
        OSHEntityTimeUtils.getModificationTimestamps(hnode),
        OSHEntityTimeUtils.getModificationTimestamps(hnode, true));
  }

  @Test
  void testGetModificationTimestampsNodeWithFilter() throws IOException {
    OSHNode hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 3, 3L, 3L, 1, new int[] {1, 2},
            86756350, 494186210),
        OSM.node(123L, 2, 2L, 2L, 1, new int[] {1, 2},
            86756350, 494186210),
        OSM.node(123L, 1, 1L, 1L, 1, new int[] {1, 1},
            86756350, 494186210)
    ));

    List<OSHDBTimestamp> tss =
        OSHEntityTimeUtils.getModificationTimestamps(hnode, e -> e.getTags().hasTag(1, 1));

    assertNotNull(tss);
    assertEquals(2, tss.size());
    assertEquals(1L, tss.get(0).getEpochSecond());
    assertEquals(2L, tss.get(1).getEpochSecond());

    // make sure that if no filter is supplied, the full result is returned
    assertEquals(
        OSHEntityTimeUtils.getModificationTimestamps(hnode),
        OSHEntityTimeUtils.getModificationTimestamps(hnode, null));
  }

  @Test
  void testGetModificationTimestampsWay() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, -3, 14L, 13L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 2, 2L, 12L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 1, 1L, 11L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(124L, 5, 14L, 25L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 4, 12L, 24L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 3, 8L, 23L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 2, 4L, 22L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 1, 3L, 21L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(125L, 3, 9L, 33L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 2, 6L, 32L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 1, 1L, 31L, 0, new int[]{}, 0, 0)
    ));

    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(123, -3, 13L, 4446L, 23, new int[]{}, new OSMMember[]{}),
        OSM.way(123, 2, 7L,
        4445L, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0)}),
        OSM.way(123, 1, 5L,
        4444L, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0),
            new OSMMember(125, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2, hnode3));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hway, false);
    assertNotNull(tss);
    assertEquals(3, tss.size());
    assertEquals(5L, tss.get(0).getEpochSecond());
    assertEquals(7L, tss.get(1).getEpochSecond());
    assertEquals(13L, tss.get(2).getEpochSecond());

    tss = OSHEntityTimeUtils.getModificationTimestamps(hway, true);
    assertNotNull(tss);
    assertEquals(6, tss.size());
    assertEquals(5L, tss.get(0).getEpochSecond());
    assertEquals(6L, tss.get(1).getEpochSecond());
    assertEquals(7L, tss.get(2).getEpochSecond());
    assertEquals(8L, tss.get(3).getEpochSecond());
    assertEquals(12L, tss.get(4).getEpochSecond());
    assertEquals(13L, tss.get(5).getEpochSecond());
  }

  @Test
  void testGetModificationTimestampsWayWithFilter() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 2, 2L, 12L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 1, 1L, 11L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(124L, 5, 16L, 25L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 4, 12L, 24L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 3, 8L, 23L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 2, 4L, 22L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 1, 3L, 21L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(125L, 4, 15L, 34L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 3, 9L, 33L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 2, 6L, 32L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 1, 1L, 31L, 0, new int[]{}, 0, 0)
    ));

    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(123, 4, 14L,
        4447L, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0)}),
        OSM.way(123, 3, 13L,
        4446L, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0)}),
        OSM.way(123, 2, 7L,
        4445L, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0)}),
        OSM.way(123, 1, 5L,
        4444L, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0),
            new OSMMember(125, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2, hnode3));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hway, true);
    assertNotNull(tss);
    assertEquals(8, tss.size());
    assertEquals(5L, tss.get(0).getEpochSecond());
    assertEquals(6L, tss.get(1).getEpochSecond());
    assertEquals(7L, tss.get(2).getEpochSecond());
    assertEquals(8L, tss.get(3).getEpochSecond());
    assertEquals(12L, tss.get(4).getEpochSecond());
    assertEquals(13L, tss.get(5).getEpochSecond());
    assertEquals(14L, tss.get(6).getEpochSecond());
    assertEquals(16L, tss.get(7).getEpochSecond());

    tss = OSHEntityTimeUtils.getModificationTimestamps(
        hway,
        osmEntity -> osmEntity.getTags().hasTag(2, 1)
    );
    assertNotNull(tss);
    assertEquals(5, tss.size());
    assertEquals(5L, tss.get(0).getEpochSecond());
    assertEquals(6L, tss.get(1).getEpochSecond());
    assertEquals(7L, tss.get(2).getEpochSecond());
    assertEquals(14L, tss.get(3).getEpochSecond());
    assertEquals(16L, tss.get(4).getEpochSecond());
  }

  @Test
  void testGetModificationTimestampsRelation() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 2, 2L, 12L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 1, 1L, 11L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(124L, 4, 12L, 24L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 3, 9L, 23L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 2, 4L, 22L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 1, 3L, 21L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(125L, 3, 11L, 34L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 2, 6L, 32L, 0, new int[]{}, 0, 0),
        OSM.node(125L, 1, 1L, 31L, 0, new int[]{}, 0, 0)
    ));

    OSHWay hway1 = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(1, 3, 7L,
        4445L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0)}),
        OSM.way(1, 2, 5L,
        4444L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0),
            new OSMMember(125, OSMType.NODE, 0)}),
        OSM.way(1, 1, 4L,
        4443L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(123, OSMType.NODE, 0),
            new OSMMember(124, OSMType.NODE, 0),
            new OSMMember(125, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2, hnode3));

    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1, -4, 20L, 10004L, 1, new int[]{}, new OSMMember[]{}),
        OSM.relation(1, 3, 10L, 10003L, 1, new int[]{1, 1, 2, 2},
            new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)}),
        OSM.relation(1, 2, 8L,
        10002L, 1, new int[]{1, 1, 2, 2},
            new OSMMember[]{new OSMMember(123, OSMType.NODE, 1)}),
        OSM.relation(1, 1, 5L,
        10001L, 1, new int[]{1, 1, 2, 2},
            new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)})
    ), List.of(hnode1), List.of(hway1));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hrelation, false);
    assertNotNull(tss);
    assertEquals(4, tss.size());
    assertEquals(5L, tss.get(0).getEpochSecond());
    assertEquals(8L, tss.get(1).getEpochSecond());
    assertEquals(10L, tss.get(2).getEpochSecond());
    assertEquals(20L, tss.get(3).getEpochSecond());

    tss = OSHEntityTimeUtils.getModificationTimestamps(hrelation, true);
    assertNotNull(tss);
    assertEquals(7, tss.size());
    assertEquals(5L, tss.get(0).getEpochSecond());
    assertEquals(6L, tss.get(1).getEpochSecond());
    assertEquals(7L, tss.get(2).getEpochSecond());
    assertEquals(8L, tss.get(3).getEpochSecond());
    // timestamp 9 has to be missing because at the time, the way wasn't a member of the
    // relation anymore
    assertEquals(10L,  tss.get(4).getEpochSecond());
    // timestamp 11 has to be missing because at the time, the node wasn't part of the way
    // member of the relation anymore
    assertEquals(12L, tss.get(5).getEpochSecond());
    assertEquals(20L, tss.get(6).getEpochSecond());
  }

  @Test
  void testGetModificationTimestampsRelationWithFilter() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 7, 17L, 17L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 6, 6L, 16L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 5, 5L, 15L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 4, 4L, 14L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 3, 3L, 13L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 2, 2L, 12L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 1, 1L, 11L, 0, new int[]{}, 0, 0)
    ));

    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1, -4, 6L, 10004L, 1, new int[]{}, new OSMMember[]{}),
        OSM.relation(1, 3, 5L,
        10003L, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0)}),
        OSM.relation(1, 2, 3L,
        10002L, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 1)}),
        OSM.relation(1, 1, 1L,
        10001L, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0)})
    ), List.of(hnode1), List.of());

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(
        hrelation,
        entity -> entity.getVersion() != 2
    );
    assertNotNull(tss);
    assertEquals(5, tss.size());
    assertEquals(1L, tss.get(0).getEpochSecond());
    assertEquals(2L, tss.get(1).getEpochSecond());
    assertEquals(3L, tss.get(2).getEpochSecond());
    // ts 4 missing since entity filter doesn't match then
    assertEquals(5L, tss.get(3).getEpochSecond());
    assertEquals(6L, tss.get(4).getEpochSecond());
  }

  @Test
  void testIssue325() throws IOException {
    // tests that the bug reported in https://github.com/GIScience/oshdb/issues/325 is fixed:
    // relations referencing redacted ways caused a crash in the OSHEntities utility class
    // when calculating the relation's modification timestamps
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 2, 2L, 2L, 0, new int[]{}, 0, 0),
        OSM.node(123L, 1, 1L, 1L, 0, new int[]{}, 0, 0)));
    OSHNode hnode2 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(124L, 2, 2L, 2L, 0, new int[]{}, 0, 0),
        OSM.node(124L, 1, 1L, 1L, 0, new int[]{}, 0, 0)));

    OSHWay hway1 = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(1, 1, 1L, 1L, 0, new int[]{}, new OSMMember[]{
            new OSMMember(123L, OSMType.NODE, 0),
            new OSMMember(124L, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2));

    OSHWay hway2 = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(2L, -4, 9L, 9L, 9, new int[]{}, new OSMMember[]{})
    ), Collections.emptyList());

    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1L, 2, 8L, 8L, 8, new int[]{1, 1, 2, 2},
            new OSMMember[]{
                new OSMMember(1L, OSMType.WAY, 0),
                new OSMMember(2L, OSMType.WAY, 0)}),
        OSM.relation(1L, 1, 1L, 1L, 0, new int[]{1, 1, 2, 2},
            new OSMMember[]{new OSMMember(1L, OSMType.WAY, 0)})
    ), List.of(hnode1, hnode2), List.of(hway1, hway2));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hrelation, true);
    assertNotNull(tss);
  }

  @Test
  void testGetChangesetTimestampsNode() throws IOException {
    OSHNode hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(123L, 2, 2L, 8L, 1, new int[] {1, 1},
            86756350, 494186210),
        OSM.node(123L, 1, 1L, 1L, 1, new int[] {1, 2},
            86756350, 494186210)
    ));

    var tss = OSHEntityTimeUtils.getChangesetTimestamps(hnode);
    assertNotNull(tss);
    assertEquals(2, tss.size());
    assertEquals(1L, tss.get(new OSHDBTimestamp(1L)).longValue());
    assertEquals(8L, tss.get(new OSHDBTimestamp(2L)).longValue());
  }

  @Test
  void testGetChangesetTimestampsWay() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(1L, 3, 5L, 5L, 1, new int[] {},
            86756340, 494186210),
        OSM.node(1L, 2, 3L, 3L, 1, new int[] {},
            86756340, 494186210),
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {},
            86756340, 494186200)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(2L, 1, 1L, 1L, 1, new int[] {},
            86756380, 494186210)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(3L, 1, 4L, 4L, 1, new int[] {},
            86756390, 494186210)
    ));

    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(1L, 3, 4L, 4L, 1, new int[] {1, 2}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0),
            new OSMMember(3L, OSMType.NODE, 0)
        }),
        OSM.way(1L, 2, 2L, 2L, 1, new int[] {1, 2}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0)
        }),
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0)
        })
    ), List.of(hnode1, hnode2, hnode3));

    /* t1 = way created
     * t2 = only way tags changed
     * t3 = only node changed
     * t4 = node added/removed
     * (t5) = removed node changed again
     */

    var tss = OSHEntityTimeUtils.getChangesetTimestamps(hway);
    assertNotNull(tss);
    assertTrue(tss.size() >= 4);
    assertEquals(1L, tss.get(new OSHDBTimestamp(1L)).longValue());
    assertEquals(2L, tss.get(new OSHDBTimestamp(2L)).longValue());
    assertEquals(3L, tss.get(new OSHDBTimestamp(3L)).longValue());
    assertEquals(4L, tss.get(new OSHDBTimestamp(4L)).longValue());
  }

  @Test
  void testGetChangesetTimestampsRelation() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(1L, 2, 3L, 3L, 1, new int[] {},
            86756340, 494186210),
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {},
            86756340, 494186200)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(2L, 1, 1L, 1L, 1, new int[] {},
            86756380, 494186210)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(3L, 1, 4L, 4L, 1, new int[] {},
            86756390, 494186210)
    ));
    OSHNode hnode4 = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(4L, 3, 8L, 8L, 1, new int[] {1, 1},
            86756340, 494186210),
        OSM.node(4L, 2, 6L, 6L, 1, new int[] {2, 2},
            86756340, 494186210),
        OSM.node(4L, 1, 1L, 1L, 1, new int[] {1, 1},
            86756390, 494186210)
    ));

    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(1L, 1, 4L, 4L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0),
            new OSMMember(3L, OSMType.NODE, 0)
        }),
        OSM.way(1L, 1, 2L, 2L, 1, new int[] {1, 2}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0)
        }),
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 2}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0)
        })
    ), List.of(hnode1, hnode2, hnode3));

    OSHRelation hrel = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1L, 3, 7L, 7L, 1, new int[] {1, 2}, new OSMMember[] {
            new OSMMember(1L, OSMType.WAY, 0)
        }),
        OSM.relation(1L, 2, 5L, 5L, 1, new int[] {1, 2}, new OSMMember[] {
            new OSMMember(1L, OSMType.WAY, 0),
            new OSMMember(4L, OSMType.NODE, 0)
        }),
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.WAY, 0),
            new OSMMember(4L, OSMType.NODE, 0)
        })
    ), List.of(hnode4), List.of(hway));

    /* t1 = relation with way and node created
     * t2 = only way tags changed
     * t3 = only node of way changed
     * t4 = only node added to way
     * t5 = only rel tags changed
     * t6 = only node of relation changed
     * t7 = node dropped from relation
     * (t8) = removed node changed again
     */

    var tss = OSHEntityTimeUtils.getChangesetTimestamps(hrel);
    assertNotNull(tss);
    assertTrue(tss.size() >= 7);
    assertEquals(1L, tss.get(new OSHDBTimestamp(1L)).longValue());
    assertEquals(2L, tss.get(new OSHDBTimestamp(2L)).longValue());
    assertEquals(3L, tss.get(new OSHDBTimestamp(3L)).longValue());
    assertEquals(4L, tss.get(new OSHDBTimestamp(4L)).longValue());
    assertEquals(5L, tss.get(new OSHDBTimestamp(5L)).longValue());
    assertEquals(6L, tss.get(new OSHDBTimestamp(6L)).longValue());
    assertEquals(7L, tss.get(new OSHDBTimestamp(7L)).longValue());
  }

  @Test
  void testGetModificationTimestampsBrokenData() throws IOException {
    // missing way node reference
    OSHNode hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {},
            86756380, 494186210)
    ));

    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0)
        })
    ), List.of(hnode));

    var tss = OSHEntityTimeUtils.getModificationTimestamps(hway);
    assertNotNull(tss);
    assertEquals(1, tss.size());
    assertEquals(1L, tss.get(0).getEpochSecond());

    // missing relation member
    OSHRelation hrel = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.WAY, 0),
            new OSMMember(3L, OSMType.NODE, 0)
        })
    ), List.of(hnode), List.of(hway));

    tss = OSHEntityTimeUtils.getModificationTimestamps(hrel);
    assertNotNull(tss);
    assertEquals(1, tss.size());
    assertEquals(1L, tss.get(0).getEpochSecond());

    // broken reference (potentially due to data redaction)
    hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {},
            86756380, 494186210)
    ));
  }

  @Test
  void testGetChangesetTimestampsBrokenData() throws IOException {
    // missing way node reference
    OSHNode hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {},
            86756380, 494186210)
    ));

    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        OSM.way(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.NODE, 0),
            new OSMMember(2L, OSMType.NODE, 0)
        })
    ), List.of(hnode));

    var tss = OSHEntityTimeUtils.getChangesetTimestamps(hway);
    assertNotNull(tss);
    assertTrue(tss.size() >= 1);
    assertEquals(1L, tss.get(new OSHDBTimestamp(1L)).longValue());

    // missing relation member
    OSHRelation hrel = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.WAY, 0),
            new OSMMember(3L, OSMType.NODE, 0)
        })
    ), List.of(hnode), List.of(hway));

    tss = OSHEntityTimeUtils.getChangesetTimestamps(hrel);
    assertNotNull(tss);
    assertTrue(tss.size() >= 1);
    assertEquals(1L, tss.get(new OSHDBTimestamp(1L)).longValue());
  }

  @Test
  void testGetModificationTimestampsNestedRelations() throws IOException {
    OSHNode hnode = OSHNodeImpl.build(Lists.newArrayList(
        OSM.node(1L, 1, 1L, 1L, 1, new int[] {},
            86756380, 494186210)
    ));
    OSHRelation hrel = OSHRelationImpl.build(Lists.newArrayList(
        OSM.relation(1L, 1, 1L, 1L, 1, new int[] {1, 1}, new OSMMember[] {
            new OSMMember(1L, OSMType.WAY, 0),
            new OSMMember(2L, OSMType.RELATION, 0)
        })
    ), List.of(hnode), Collections.emptyList());

    var tss = OSHEntityTimeUtils.getModificationTimestamps(hrel);
    assertNotNull(tss);
    assertEquals(1, tss.size());
    assertEquals(1L, tss.get(0).getEpochSecond());
  }
}
