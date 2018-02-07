package org.heigit.bigspatialdata.oshdb.osh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

import static org.junit.Assert.*;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.junit.Test;

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
      OSHRelation hrelation = OSHRelation.build(versions, Arrays.asList(node100, node102, node104), Collections.emptyList());

      List<OSHNode> nodes = hrelation.getNodes();
      assertTrue(nodes.size() == 3);

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelation.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testWithMissingNode() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    try {
      OSHRelation hrelation = OSHRelation.build(versions, Arrays.asList(node100, node104), Collections.emptyList());

      List<OSHNode> nodes = hrelation.getNodes();
      assertTrue(nodes.size() == 2);

      Iterator<OSMRelation> itr = hrelation.iterator();
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
      fail("HOSMRelation.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testGetWays() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(200, OSMType.WAY, 0), new OSMMember(202, OSMType.WAY, 0)}));

    try {
      OSHRelation hrelation = OSHRelation.build(versions, Collections.emptyList(), Arrays.asList(way200, way202), 200l, 1000l, 1000l, 1000l);

      List<OSHWay> ways = hrelation.getWays();
      assertTrue(ways.size() == 2);

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelation.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testCompact() {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0), new OSMMember(200, OSMType.WAY, 0), new OSMMember(202, OSMType.WAY, 0)}));

    try {
      OSHRelation hrelation = OSHRelation.build(versions, Arrays.asList(node100, node102, node104), Arrays.asList(way200, way202), 200l, 1000l, 1000l, 1000l);

      List<OSHNode> nodes = hrelation.getNodes();
      assertTrue(nodes.size() == 3);

      OSHNode node;
      node = nodes.get(0);
      assertEquals(node.getId(), 100);
      assertEquals(node.getVersions().get(0).getLon(), node100.getVersions().get(0).getLon());

      node = nodes.get(1);
      assertEquals(node.getId(), 102);
      assertEquals(node.getVersions().get(0).getLon(), node100.getVersions().get(0).getLon());

      node = nodes.get(2);
      assertEquals(node.getId(), 104);
      assertEquals(node.getVersions().get(0).getLon(), node100.getVersions().get(0).getLon());

      List<OSHWay> ways = hrelation.getWays();
      assertTrue(ways.size() == 2);

      OSHWay way;
      way = ways.get(0);
      assertEquals(way.getId(), 200);
      assertEquals(way.getNodes().get(0).getVersions().get(0).getLon(), way200.getNodes().get(0).getVersions().get(0).getLon());

    } catch (IOException e) {
      e.printStackTrace();
      fail("HOSMRelation.build Exception: " + e.getMessage());
    }
  }

  @Test
  public void testGetModificationTimestamps() throws IOException {
    List<OSMNode> n1versions = new ArrayList<>();
    n1versions.add(new OSMNode(123l, 2, new OSHDBTimestamp(2l), 12l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 1, new OSHDBTimestamp(1l), 11l, 0, new int[]{}, 0, 0));
    OSHNode hnode1 = OSHNode.build(n1versions);
    List<OSMNode> n2versions = new ArrayList<>();
    n2versions.add(new OSMNode(124l, 4, new OSHDBTimestamp(12l), 24l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 3, new OSHDBTimestamp(8l), 23l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 2, new OSHDBTimestamp(4l), 22l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 1, new OSHDBTimestamp(3l), 21l, 0, new int[]{}, 0, 0));
    OSHNode hnode2 = OSHNode.build(n2versions);
    List<OSMNode> n3versions = new ArrayList<>();
    n3versions.add(new OSMNode(125l, 4, new OSHDBTimestamp(11l), 34l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 3, new OSHDBTimestamp(9l), 33l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 2, new OSHDBTimestamp(6l), 32l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 1, new OSHDBTimestamp(1l), 31l, 0, new int[]{}, 0, 0));
    OSHNode hnode3 = OSHNode.build(n3versions);

    List<OSMWay> w1versions = new ArrayList<>();
    w1versions.add(new OSMWay(1, 2, new OSHDBTimestamp(7l), 4445l, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}));
    w1versions.add(new OSMWay(1, 1, new OSHDBTimestamp(5l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)}));
    OSHWay hway1 = OSHWay.build(w1versions, Arrays.asList(hnode1, hnode2, hnode3));

    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(1, 3, new OSHDBTimestamp(10l), 10000l, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)}));
    versions.add(new OSMRelation(1, 2, new OSHDBTimestamp(8l), 10000l, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{}));
    versions.add(new OSMRelation(1, 1, new OSHDBTimestamp(5l), 10000l, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(1, OSMType.WAY, 0)}));
    OSHRelation hrelation = OSHRelation.build(versions, Arrays.asList(hnode1, hnode2, hnode3), Arrays.asList(hway1));

    List<OSHDBTimestamp> tss = hrelation.getModificationTimestamps(false);
    assertNotNull(tss);
    assertEquals(3, tss.size());
    assertEquals(5l, tss.get(0).getRawUnixTimestamp());
    assertEquals(8l, tss.get(1).getRawUnixTimestamp());
    assertEquals(10l, tss.get(2).getRawUnixTimestamp());

    tss = hrelation.getModificationTimestamps(true);
    assertNotNull(tss);
    assertEquals(6, tss.size());
    assertEquals(5l, tss.get(0).getRawUnixTimestamp());
    assertEquals(6l, tss.get(1).getRawUnixTimestamp());
    assertEquals(7l, tss.get(2).getRawUnixTimestamp());
    assertEquals(8l, tss.get(3).getRawUnixTimestamp());
    // timestamp 9 has to be missing because at the time, the way wasn't a member of the relation anymore
    assertEquals(10l,  tss.get(4).getRawUnixTimestamp());
    // timestamp 11 has to be missing because at the time, the node wasn't part of the way member of the relation anymore
    assertEquals(12l, tss.get(5).getRawUnixTimestamp());
  }

  static OSHNode buildHOSMNode(List<OSMNode> versions) {
    try {
      return OSHNode.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  static OSHWay buildHOSMWay(List<OSMWay> versions, List<OSHNode> nodes) {
    try {
      return OSHWay.build(versions, nodes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testToString() throws IOException {
    List<OSMRelation> versions = new ArrayList<>();
    versions.add(new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    OSHRelation instance = OSHRelation.build(versions, Arrays.asList(node100, node102, node104), Collections.emptyList());
    String expResult = "OSHRelation ID:300 Vmax:+1+ Creation:3333 BBox:(8.680973,49.409498),(8.680973,49.409498)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
