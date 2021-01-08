package org.heigit.ohsome.oshdb.util.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
import org.junit.Test;

public class TestOSHEntityTimeUtils {
  @Test
  public void testGetModificationTimestampsNode() throws IOException {
    OSHNode hnode = OSHNodeImpl.build(List.of(
        new OSMNode(123L, 2, new OSHDBTimestamp(2L), 0L, 1, new int[] {1, 1}, new long[] {86756350L, 494186210L}[0], new long[] {86756350L, 494186210L}[1]),
        new OSMNode(123L, 1, new OSHDBTimestamp(1L), 0L, 1, new int[] {1, 1}, new long[] {86756350L, 494186210L}[0], new long[] {86756350L, 494186210L}[1])
    ));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hnode);
    assertNotNull(tss);
    assertEquals(2, tss.size());
    assertEquals(1L, tss.get(0).getRawUnixTimestamp());
    assertEquals(2L, tss.get(1).getRawUnixTimestamp());
  }

  @Test
  public void testGetModificationTimestampsWay() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(List.of(
      new OSMNode(123L, -3, new OSHDBTimestamp(14L), 13L, 0, new int[]{}, 0, 0),
      new OSMNode(123L, 2, new OSHDBTimestamp(2L), 12L, 0, new int[]{}, 0, 0),
      new OSMNode(123L, 1, new OSHDBTimestamp(1L), 11L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(List.of(
        new OSMNode(124L, 5, new OSHDBTimestamp(14L), 25L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 4, new OSHDBTimestamp(12L), 24L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 3, new OSHDBTimestamp(8L), 23L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 2, new OSHDBTimestamp(4L), 22L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 1, new OSHDBTimestamp(3L), 21L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(List.of(
        new OSMNode(125L, 3, new OSHDBTimestamp(9L), 33L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 2, new OSHDBTimestamp(6L), 32L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 1, new OSHDBTimestamp(1L), 31L, 0, new int[]{}, 0, 0)
    ));

    OSHWay hway = OSHWayImpl.build(List.of(
        new OSMWay(123, -3, new OSHDBTimestamp(13L), 4446L, 23, new int[]{}, new OSMMember[]{}),
        new OSMWay(123, 2, new OSHDBTimestamp(7L),
        4445L, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}),
        new OSMWay(123, 1, new OSHDBTimestamp(5L),
        4444L, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2, hnode3));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hway, false);
    assertNotNull(tss);
    assertEquals(3, tss.size());
    assertEquals(5L, tss.get(0).getRawUnixTimestamp());
    assertEquals(7L, tss.get(1).getRawUnixTimestamp());
    assertEquals(13L, tss.get(2).getRawUnixTimestamp());

    tss = OSHEntityTimeUtils.getModificationTimestamps(hway, true);
    assertNotNull(tss);
    assertEquals(6, tss.size());
    assertEquals(5L, tss.get(0).getRawUnixTimestamp());
    assertEquals(6L, tss.get(1).getRawUnixTimestamp());
    assertEquals(7L, tss.get(2).getRawUnixTimestamp());
    assertEquals(8L, tss.get(3).getRawUnixTimestamp());
    assertEquals(12L, tss.get(4).getRawUnixTimestamp());
    assertEquals(13L, tss.get(5).getRawUnixTimestamp());
  }

  @Test
  public void testGetModificationTimestampsWayWithFilter() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(List.of(
        new OSMNode(123L, 2, new OSHDBTimestamp(2L), 12L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 1, new OSHDBTimestamp(1L), 11L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(List.of(
        new OSMNode(124L, 5, new OSHDBTimestamp(16L), 25L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 4, new OSHDBTimestamp(12L), 24L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 3, new OSHDBTimestamp(8L), 23L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 2, new OSHDBTimestamp(4L), 22L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 1, new OSHDBTimestamp(3L), 21L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(List.of(
        new OSMNode(125L, 4, new OSHDBTimestamp(15L), 34L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 3, new OSHDBTimestamp(9L), 33L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 2, new OSHDBTimestamp(6L), 32L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 1, new OSHDBTimestamp(1L), 31L, 0, new int[]{}, 0, 0)
    ));

    OSHWay hway = OSHWayImpl.build(List.of(
        new OSMWay(123, 4, new OSHDBTimestamp(14L),
        4447L, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}),
        new OSMWay(123, 3, new OSHDBTimestamp(13L),
        4446L, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}),
        new OSMWay(123, 2, new OSHDBTimestamp(7L),
        4445L, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}),
        new OSMWay(123, 1, new OSHDBTimestamp(5L),
        4444L, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2, hnode3));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hway, true);
    assertNotNull(tss);
    assertEquals(8, tss.size());
    assertEquals(5L, tss.get(0).getRawUnixTimestamp());
    assertEquals(6L, tss.get(1).getRawUnixTimestamp());
    assertEquals(7L, tss.get(2).getRawUnixTimestamp());
    assertEquals(8L, tss.get(3).getRawUnixTimestamp());
    assertEquals(12L, tss.get(4).getRawUnixTimestamp());
    assertEquals(13L, tss.get(5).getRawUnixTimestamp());
    assertEquals(14L, tss.get(6).getRawUnixTimestamp());
    assertEquals(16L, tss.get(7).getRawUnixTimestamp());

    tss = OSHEntityTimeUtils.getModificationTimestamps(
        hway,
        osmEntity -> osmEntity.hasTagValue(2, 1)
    );
    assertNotNull(tss);
    assertEquals(5, tss.size());
    assertEquals(5L, tss.get(0).getRawUnixTimestamp());
    assertEquals(6L, tss.get(1).getRawUnixTimestamp());
    assertEquals(7L, tss.get(2).getRawUnixTimestamp());
    assertEquals(14L, tss.get(3).getRawUnixTimestamp());
    assertEquals(16L, tss.get(4).getRawUnixTimestamp());
  }

  @Test
  public void testGetModificationTimestampsRelation() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(List.of(
        new OSMNode(123L, 2, new OSHDBTimestamp(2L), 12L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 1, new OSHDBTimestamp(1L), 11L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode2 = OSHNodeImpl.build(List.of(
        new OSMNode(124L, 4, new OSHDBTimestamp(12L), 24L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 3, new OSHDBTimestamp(9L), 23L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 2, new OSHDBTimestamp(4L), 22L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 1, new OSHDBTimestamp(3L), 21L, 0, new int[]{}, 0, 0)
    ));
    OSHNode hnode3 = OSHNodeImpl.build(List.of(
        new OSMNode(125L, 3, new OSHDBTimestamp(11L), 34L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 2, new OSHDBTimestamp(6L), 32L, 0, new int[]{}, 0, 0),
        new OSMNode(125L, 1, new OSHDBTimestamp(1L), 31L, 0, new int[]{}, 0, 0)
    ));

    OSHWay hway1 = OSHWayImpl.build(List.of(
        new OSMWay(1, 3, new OSHDBTimestamp(7L),
        4445L, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}),
        new OSMWay(1, 2, new OSHDBTimestamp(5L),
        4444L, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)}),
        new OSMWay(1, 1, new OSHDBTimestamp(4L),
        4443L, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)})
    ), List.of(hnode1, hnode2, hnode3));

    OSHRelation hrelation = OSHRelationImpl.build(List.of(
        new OSMRelation(1, -4, new OSHDBTimestamp(20L), 10004L, 1, new int[]{}, new OSMMember[]{}),
        new OSMRelation(1, 3, new OSHDBTimestamp(10L), 10003L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)}),
        new OSMRelation(1, 2, new OSHDBTimestamp(8L),
        10002L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 1)}),
        new OSMRelation(1, 1, new OSHDBTimestamp(5L),
        10001L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)})
    ), List.of(hnode1, hnode2, hnode3), List.of(hway1));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hrelation,false);
    assertNotNull(tss);
    assertEquals(4, tss.size());
    assertEquals(5L, tss.get(0).getRawUnixTimestamp());
    assertEquals(8L, tss.get(1).getRawUnixTimestamp());
    assertEquals(10L, tss.get(2).getRawUnixTimestamp());
    assertEquals(20L, tss.get(3).getRawUnixTimestamp());

    tss = OSHEntityTimeUtils.getModificationTimestamps(hrelation,true);
    assertNotNull(tss);
    assertEquals(7, tss.size());
    assertEquals(5L, tss.get(0).getRawUnixTimestamp());
    assertEquals(6L, tss.get(1).getRawUnixTimestamp());
    assertEquals(7L, tss.get(2).getRawUnixTimestamp());
    assertEquals(8L, tss.get(3).getRawUnixTimestamp());
    // timestamp 9 has to be missing because at the time, the way wasn't a member of the relation anymore
    assertEquals(10L,  tss.get(4).getRawUnixTimestamp());
    // timestamp 11 has to be missing because at the time, the node wasn't part of the way member of the relation anymore
    assertEquals(12L, tss.get(5).getRawUnixTimestamp());
    assertEquals(20L, tss.get(6).getRawUnixTimestamp());
  }

  @Test
  public void testGetModificationTimestampsRelationWithFilter() throws IOException {
    OSHNode hnode1 = OSHNodeImpl.build(List.of(
        new OSMNode(123L, 7, new OSHDBTimestamp(17L), 17L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 6, new OSHDBTimestamp(6L), 16L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 5, new OSHDBTimestamp(5L), 15L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 4, new OSHDBTimestamp(4L), 14L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 3, new OSHDBTimestamp(3L), 13L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 2, new OSHDBTimestamp(2L), 12L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 1, new OSHDBTimestamp(1L), 11L, 0, new int[]{}, 0, 0)
    ));

    OSHRelation hrelation = OSHRelationImpl.build(List.of(
        new OSMRelation(1, -4, new OSHDBTimestamp(6L), 10004L, 1, new int[]{}, new OSMMember[]{}),
        new OSMRelation(1, 3, new OSHDBTimestamp(5L),
        10003L, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0)}),
        new OSMRelation(1, 2, new OSHDBTimestamp(3L),
        10002L, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 1)}),
        new OSMRelation(1, 1, new OSHDBTimestamp(1L),
        10001L, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0)})
    ), List.of(hnode1), List.of());

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(
        hrelation,
        entity -> entity.getVersion() != 2
    );
    assertNotNull(tss);
    assertEquals(5, tss.size());
    assertEquals(1L, tss.get(0).getRawUnixTimestamp());
    assertEquals(2L, tss.get(1).getRawUnixTimestamp());
    assertEquals(3L, tss.get(2).getRawUnixTimestamp());
    // ts 4 missing since entity filter doesn't match then
    assertEquals(5L, tss.get(3).getRawUnixTimestamp());
    assertEquals(6L, tss.get(4).getRawUnixTimestamp());
  }

  @Test
  public void testIssue325() throws IOException {
    // tests that the bug reported in https://github.com/GIScience/oshdb/issues/325 is fixed:
    // relations referencing redacted ways caused a crash in the OSHEntities utility class
    // when calculating the relation's modification timestamps
    OSHNode hnode1 = OSHNodeImpl.build(new ArrayList<>(List.of(
        new OSMNode(123L, 2, new OSHDBTimestamp(2L), 2L, 0, new int[]{}, 0, 0),
        new OSMNode(123L, 1, new OSHDBTimestamp(1L), 1L, 0, new int[]{}, 0, 0))));
    OSHNode hnode2 = OSHNodeImpl.build(new ArrayList<>(List.of(
        new OSMNode(124L, 2, new OSHDBTimestamp(2L), 2L, 0, new int[]{}, 0, 0),
        new OSMNode(124L, 1, new OSHDBTimestamp(1L), 1L, 0, new int[]{}, 0, 0))));

    OSHWay hway1 = OSHWayImpl.build(new ArrayList<>(List.of(
        new OSMWay(1, 1, new OSHDBTimestamp(1L), 1L, 0, new int[]{}, new OSMMember[]{
            new OSMMember(123L, OSMType.NODE, 0),
            new OSMMember(124L, OSMType.NODE, 0)})
    )), List.of(hnode1, hnode2));

    OSHWay hway2 = OSHWayImpl.build(new ArrayList<>(List.of(
        new OSMWay(2L, -4, new OSHDBTimestamp(9L), 9L, 9, new int[]{}, new OSMMember[]{})
    )), Collections.emptyList());

    OSHRelation hrelation = OSHRelationImpl.build(new ArrayList<>(List.of(
        new OSMRelation(1L, 2, new OSHDBTimestamp(8L), 8L, 8, new int[]{1, 1, 2, 2}, new OSMMember[]{
            new OSMMember(1L, OSMType.WAY, 0),
            new OSMMember(2L, OSMType.WAY, 0)}),
        new OSMRelation(1L, 1, new OSHDBTimestamp(1L), 1L, 0, new int[]{1, 1, 2, 2}, new OSMMember[]{
            new OSMMember(1L, OSMType.WAY, 0)})
    )), List.of(hnode1, hnode2), List.of(hway1, hway2));

    List<OSHDBTimestamp> tss = OSHEntityTimeUtils.getModificationTimestamps(hrelation, true);
    assertNotNull(tss);
  }
}
