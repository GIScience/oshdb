package org.heigit.bigspatialdata.oshdb.osh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class OSHWayTest {

  OSHNode node100 = buildHOSMNode(
          Arrays.asList(new OSMNode(100l, 1, 1l, 0l, 123, new int[]{1, 2}, 494094984l, 86809727l)));
  OSHNode node102 = buildHOSMNode(
          Arrays.asList(new OSMNode(102l, 1, 1l, 0l, 123, new int[]{2, 1}, 494094984l, 86809727l)));
  OSHNode node104 = buildHOSMNode(
          Arrays.asList(new OSMNode(104l, 1, 1l, 0l, 123, new int[]{2, 4}, 494094984l, 86809727l)));

  @Test
  public void testGetNodes() throws IOException {
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
            new OSMWay(123, 1, 3333l, 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
    versions.add(
            new OSMWay(123, 3, 3333l, 4444l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    OSHWay hway = OSHWay.build(versions, Arrays.asList(node100, node102, node104));
    assertNotNull(hway);

    List<OSHNode> nodes = hway.getNodes();
    assertEquals(3, nodes.size());

  }

  @Test
  public void testCreateGeometrey() throws IOException {
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
            new OSMWay(123, 1, 3333l, 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
    versions.add(
            new OSMWay(123, 3, 3333l, 4444l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    OSHWay hway = OSHWay.build(versions, Arrays.asList(node100, node102, node104));
    assertNotNull(hway);

    Iterator<OSMWay> ways = hway.iterator();

    OSMWay w = ways.next();

    OSMMember[] members = w.getRefs();
    members[0].getEntity();

    List<OSHNode> nodes = hway.getNodes();

  }

  @Test
  public void testWithMissingNode() throws IOException {
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
            new OSMWay(123, 3, 3333l, 4444l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
    versions.add(
            new OSMWay(123, 1, 3333l, 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    OSHWay hway = OSHWay.build(versions, Arrays.asList(node100, node104));
    assertNotNull(hway);

    List<OSHNode> nodes = hway.getNodes();
    assertEquals(2, nodes.size());

    OSMWay way;
    OSMMember[] members;
    Iterator<OSMWay> itr = hway.iterator();
    assertTrue(itr.hasNext());
    way = itr.next();
    members = way.getRefs();
    assertEquals(2, members.length);
    assertEquals(100, members[0].getId());
    assertEquals(104, members[1].getId());

    assertTrue(itr.hasNext());
    way = itr.next();
    members = way.getRefs();
    assertEquals(2, members.length);

    assertEquals(102, members[0].getId());
    assertEquals(104, members[1].getId());
  }

  @Test
  public void testGetModificationTimestamps() throws IOException {
    List<OSMNode> n1versions = new ArrayList<>();
    n1versions.add(new OSMNode(123l, 2, 2l, 12l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 1, 1l, 11l, 0, new int[]{}, 0, 0));
    OSHNode hnode1 = OSHNode.build(n1versions);
    List<OSMNode> n2versions = new ArrayList<>();
    n2versions.add(new OSMNode(124l, 4, 12l, 24l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 3, 8l, 23l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 2, 4l, 22l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 1, 3l, 21l, 0, new int[]{}, 0, 0));
    OSHNode hnode2 = OSHNode.build(n2versions);
    List<OSMNode> n3versions = new ArrayList<>();
    n3versions.add(new OSMNode(125l, 3, 9l, 33l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 2, 6l, 32l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 1, 1l, 31l, 0, new int[]{}, 0, 0));
    OSHNode hnode3 = OSHNode.build(n3versions);

    List<OSMWay> versions = new ArrayList<>();
    versions.add(new OSMWay(123, 2, 7l, 4445l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}));
    versions.add(new OSMWay(123, 1, 5l, 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)}));
    OSHWay hway = OSHWay.build(versions, Arrays.asList(hnode1, hnode2, hnode3));

    List<Long> tss = hway.getModificationTimestamps(false);
    assertNotNull(tss);
    assertEquals(2, tss.size());
    assertEquals(5l, (long) tss.get(0));
    assertEquals(7l, (long) tss.get(1));

    tss = hway.getModificationTimestamps(true);
    assertNotNull(tss);
    assertEquals(5, tss.size());
    assertEquals(5l, (long) tss.get(0));
    assertEquals(6l, (long) tss.get(1));
    assertEquals(7l, (long) tss.get(2));
    assertEquals(8l, (long) tss.get(3));
    assertEquals(12l, (long) tss.get(4));
  }

  @Test
  public void testGetModificationTimestampsWithFilter() throws IOException {
    List<OSMNode> n1versions = new ArrayList<>();
    n1versions.add(new OSMNode(123l, 2, 2l, 12l, 0, new int[]{}, 0, 0));
    n1versions.add(new OSMNode(123l, 1, 1l, 11l, 0, new int[]{}, 0, 0));
    OSHNode hnode1 = OSHNode.build(n1versions);
    List<OSMNode> n2versions = new ArrayList<>();
    n2versions.add(new OSMNode(124l, 5, 16l, 25l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 4, 12l, 24l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 3, 8l, 23l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 2, 4l, 22l, 0, new int[]{}, 0, 0));
    n2versions.add(new OSMNode(124l, 1, 3l, 21l, 0, new int[]{}, 0, 0));
    OSHNode hnode2 = OSHNode.build(n2versions);
    List<OSMNode> n3versions = new ArrayList<>();
    n3versions.add(new OSMNode(125l, 4, 15l, 34l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 3, 9l, 33l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 2, 6l, 32l, 0, new int[]{}, 0, 0));
    n3versions.add(new OSMNode(125l, 1, 1l, 31l, 0, new int[]{}, 0, 0));
    OSHNode hnode3 = OSHNode.build(n3versions);

    List<OSMWay> versions = new ArrayList<>();
    versions.add(new OSMWay(123, 4, 14l, 4447l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}));
    versions.add(new OSMWay(123, 3, 13l, 4446l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}));
    versions.add(new OSMWay(123, 2, 7l, 4445l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0)}));
    versions.add(new OSMWay(123, 1, 5l, 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(123, OSMType.NODE, 0), new OSMMember(124, OSMType.NODE, 0), new OSMMember(125, OSMType.NODE, 0)}));
    OSHWay hway = OSHWay.build(versions, Arrays.asList(hnode1, hnode2, hnode3));

    List<Long> tss = hway.getModificationTimestamps(true);
    assertNotNull(tss);
    assertEquals(8, tss.size());
    assertEquals(5l, (long) tss.get(0));
    assertEquals(6l, (long) tss.get(1));
    assertEquals(7l, (long) tss.get(2));
    assertEquals(8l, (long) tss.get(3));
    assertEquals(12l, (long) tss.get(4));
    assertEquals(13l, (long) tss.get(5));
    assertEquals(14l, (long) tss.get(6));
    assertEquals(16l, (long) tss.get(7));

    tss = hway.getModificationTimestamps(osmEntity -> osmEntity.hasTagValue(2, 1));
    assertNotNull(tss);
    //assertEquals(5, tss.size());
    assertEquals(5l, (long) tss.get(0));
    assertEquals(6l, (long) tss.get(1));
    assertEquals(7l, (long) tss.get(2));
    assertEquals(14l, (long) tss.get(3));
    assertEquals(16l, (long) tss.get(4));
  }

  static OSHNode buildHOSMNode(List<OSMNode> versions) {
    try {
      return OSHNode.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testToString() throws IOException {
    List<OSMWay> versions = new ArrayList<>();
    versions.add(
            new OSMWay(123, 1, 3333l, 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
    versions.add(
            new OSMWay(123, 3, 3333l, 4444l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));

    OSHWay instance = OSHWay.build(versions, Arrays.asList(node100, node102, node104));
    String expResult = "OSHWay ID:123 Vmax:+3+ Creation:3333 BBox:(8.680973,49.409498),(8.680973,49.409498)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
