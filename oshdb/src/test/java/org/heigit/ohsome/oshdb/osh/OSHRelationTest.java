package org.heigit.ohsome.oshdb.osh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;

import static org.junit.Assert.*;

import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
import org.junit.Test;
import com.google.common.collect.Iterables;

public class OSHRelationTest {

  OSHNode node100 = buildHOSMNode(
          Arrays.asList(new OSMNode(100l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{1, 2}, 494094984l, 86809727l)));
  OSHNode node102 = buildHOSMNode(
          Arrays.asList(new OSMNode(102l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 1}, 494094984l, 86809727l)));
  OSHNode node104 = buildHOSMNode(
          Arrays.asList(new OSMNode(104l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 4}, 494094984l, 86809727l)));

  OSHWay way200 = buildHOSMWay(Arrays.asList(new OSMWay(200, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)})), Arrays.asList(node100, node104));
  OSHWay way202 = buildHOSMWay(Arrays.asList(new OSMWay(202, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0)})), Arrays.asList(node100, node102));

  @Test
  public void testGetNodes() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    try {
      OSHRelation hrelation = OSHRelationImpl
          .build(versions, Arrays.asList(node100, node102, node104), Collections.emptyList());

      List<OSHNode> nodes = hrelation.getNodes();
      assertTrue(nodes.size() == 3);

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelationImpl.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testWithMissingNode() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    try {
      OSHRelation hrelation = OSHRelationImpl.build(versions, Arrays.asList(node100, node104), Collections.emptyList());

      List<OSHNode> nodes = hrelation.getNodes();
      assertTrue(nodes.size() == 2);

      Iterator<OSMRelation> itr = hrelation.getVersions().iterator();
      assertTrue(itr.hasNext());
      OSMRelation r = itr.next();
      assertNotNull(r);
      OSMMember[] members = r.getMembers();
      assertEquals(members.length, 3);

      assertEquals(100, members[0].getId());
      assertNotNull(members[0].getEntity());

      assertEquals(102, members[1].getId());
      assertNull(members[1].getEntity());

      assertEquals(104, members[2].getId());
      assertNotNull(members[2].getEntity());

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelationImpl.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testGetWays() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(200, OSMType.WAY, 0), new OSMMember(202, OSMType.WAY, 0)}));

    try {
      OSHRelation hrelation = OSHRelationImpl.build(versions, Collections.emptyList(), Arrays.asList(way200, way202), 200l, 1000l, 1000l, 1000l);

      List<OSHWay> ways = hrelation.getWays();
      assertTrue(ways.size() == 2);

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelationImpl.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testCompact() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0), new OSMMember(200, OSMType.WAY, 0), new OSMMember(202, OSMType.WAY, 0)}));

    try {
      OSHRelation hrelation = OSHRelationImpl.build(versions, Arrays.asList(node100, node102, node104), Arrays.asList(way200, way202), 200l, 1000l, 1000l, 1000l);

      List<OSHNode> nodes = hrelation.getNodes();
      assertTrue(nodes.size() == 3);

      OSHNode node;
      node = nodes.get(0);
      assertEquals(node.getId(), 100);
      assertEquals(Iterables.getFirst(node.getVersions(),null).getLon(), Iterables.getFirst(node100.getVersions(),null).getLon());

      node = nodes.get(1);
      assertEquals(node.getId(), 102);
      assertEquals(Iterables.getFirst(node.getVersions(),null).getLon(), Iterables.getFirst(node100.getVersions(),null).getLon());

      node = nodes.get(2);
      assertEquals(node.getId(), 104);
      assertEquals(Iterables.getFirst(node.getVersions(),null).getLon(), Iterables.getFirst(node100.getVersions(),null).getLon());

      List<OSHWay> ways = hrelation.getWays();
      assertTrue(ways.size() == 2);

      OSHWay way;
      way = ways.get(0);
      assertEquals(way.getId(), 200);
      assertEquals(Iterables.getFirst(way.getNodes().get(0).getVersions(),null).getLon(), Iterables.getFirst(way200.getNodes().get(0).getVersions(),null).getLon());

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelationImpl.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testGetModificationTimestamps() throws IOException {
    List<OSMNode> n1versions = new ArrayList<>();
    n1versions.add(new OSMNode(123l, 2, new OSHDBTimestamp(2l), 12l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 1, new OSHDBTimestamp(1l), 11l, 0, new int[]{}, 0, 0));
    OSHNode hnode1 = OSHNodeImpl.build(n1versions);
    List<OSMNode> n2versions = new ArrayList<>();
    n2versions.add(new OSMNode(124l, 4, new OSHDBTimestamp(12l), 24l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 3, new OSHDBTimestamp(9l), 23l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 2, new OSHDBTimestamp(4l), 22l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 1, new OSHDBTimestamp(3l), 21l, 0, new int[]{}, 0, 0));
    OSHNode hnode2 = OSHNodeImpl.build(n2versions);
    List<OSMNode> n3versions = new ArrayList<>();
    n3versions.add(new OSMNode(125l, 3, new OSHDBTimestamp(11l), 34l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 2, new OSHDBTimestamp(6l), 32l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 1, new OSHDBTimestamp(1l), 31l, 0, new int[]{}, 0, 0));
    OSHNode hnode3 = OSHNodeImpl.build(n3versions);

    List<OSMWay> w1versions = new ArrayList<>();
    w1versions.add(new OSMWay(1, 3, new OSHDBTimestamp(7l), 4445l, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}));
    w1versions.add(new OSMWay(1, 2, new OSHDBTimestamp(5l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)}));
    w1versions.add(new OSMWay(1, 1, new OSHDBTimestamp(4l), 4443l, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)}));
    OSHWay hway1 = OSHWayImpl.build(w1versions, Arrays.asList(hnode1, hnode2, hnode3));

    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(1, -4, new OSHDBTimestamp(20l), 10004l, 1, new int[]{}, new OSMMember[]{}));
    versions.add(new OSMRelation(1, 3, new OSHDBTimestamp(10l), 10003l, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)}));
    versions.add(new OSMRelation(1, 2, new OSHDBTimestamp(8l), 10002l, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 1)}));
    versions.add(new OSMRelation(1, 1, new OSHDBTimestamp(5l), 10001l, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)}));
    OSHRelation hrelation = OSHRelationImpl.build(versions, Arrays.asList(hnode1, hnode2, hnode3), Arrays.asList(hway1));

    List<OSHDBTimestamp> tss = OSHEntities.getModificationTimestamps(hrelation,false);
    assertNotNull(tss);
    assertEquals(4, tss.size());
    assertEquals(5l, tss.get(0).getRawUnixTimestamp());
    assertEquals(8l, tss.get(1).getRawUnixTimestamp());
    assertEquals(10l, tss.get(2).getRawUnixTimestamp());
    assertEquals(20l, tss.get(3).getRawUnixTimestamp());

    tss = OSHEntities.getModificationTimestamps(hrelation,true);
    assertNotNull(tss);
    assertEquals(7, tss.size());
    assertEquals(5l, tss.get(0).getRawUnixTimestamp());
    assertEquals(6l, tss.get(1).getRawUnixTimestamp());
    assertEquals(7l, tss.get(2).getRawUnixTimestamp());
    assertEquals(8l, tss.get(3).getRawUnixTimestamp());
    // timestamp 9 has to be missing because at the time, the way wasn't a member of the relation anymore
    assertEquals(10l,  tss.get(4).getRawUnixTimestamp());
    // timestamp 11 has to be missing because at the time, the node wasn't part of the way member of the relation anymore
    assertEquals(12l, tss.get(5).getRawUnixTimestamp());
    assertEquals(20l, tss.get(6).getRawUnixTimestamp());
  }

  @Test
  public void testGetModificationTimestampsWithFilter() throws IOException {
    List<OSMNode> n1versions = new ArrayList<>();
    n1versions.add(new OSMNode(123l, 7, new OSHDBTimestamp(17l), 17l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 6, new OSHDBTimestamp(6l), 16l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 5, new OSHDBTimestamp(5l), 15l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 4, new OSHDBTimestamp(4l), 14l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 3, new OSHDBTimestamp(3l), 13l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 2, new OSHDBTimestamp(2l), 12l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 1, new OSHDBTimestamp(1l), 11l, 0, new int[]{}, 0, 0));
    OSHNode hnode1 = OSHNodeImpl.build(n1versions);

    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(1, -4, new OSHDBTimestamp(6l), 10004l, 1, new int[]{}, new OSMMember[]{}));
    versions.add(new OSMRelation(1, 3, new OSHDBTimestamp(5l), 10003l, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0)}));
    versions.add(new OSMRelation(1, 2, new OSHDBTimestamp(3l), 10002l, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 1)}));
    versions.add(new OSMRelation(1, 1, new OSHDBTimestamp(1l), 10001l, 1, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0)}));
    OSHRelation hrelation = OSHRelationImpl.build(versions, Arrays.asList(hnode1), Arrays.asList());

    List<OSHDBTimestamp> tss = OSHEntities.getModificationTimestamps(
        hrelation,
        entity -> entity.getVersion() != 2
    );
    assertNotNull(tss);
    assertEquals(5, tss.size());
    assertEquals(1l, tss.get(0).getRawUnixTimestamp());
    assertEquals(2l, tss.get(1).getRawUnixTimestamp());
    assertEquals(3l, tss.get(2).getRawUnixTimestamp());
    // ts 4 missing since entity filter doesn't match then
    assertEquals(5l, tss.get(3).getRawUnixTimestamp());
    assertEquals(6l, tss.get(4).getRawUnixTimestamp());

  }

  static OSHNode buildHOSMNode(List<OSMNode> versions) {
    try {
      return OSHNodeImpl.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  static OSHWay buildHOSMWay(List<OSMWay> versions, List<OSHNode> nodes) {
    try {
      return OSHWayImpl.build(versions, nodes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testToString() throws IOException {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    OSHRelation instance = OSHRelationImpl.build(versions, Arrays.asList(node100, node102, node104), Collections.emptyList());
    String expResult = "OSHRelation ID:300 Vmax:+1+ Creation:3333 BBox:(8.680973,49.409498),(8.680973,49.409498)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
