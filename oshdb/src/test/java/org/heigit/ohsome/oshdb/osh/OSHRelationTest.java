package org.heigit.ohsome.oshdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;
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
import org.heigit.ohsome.oshdb.util.OSHDBTimestamp;
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
