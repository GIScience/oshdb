package org.heigit.ohsome.oshdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.junit.Test;

public class OSHWayTest {

  OSHNode node100 = OSHNodeTest.buildOSHNode(new OSMNode(
      100L, 1, 1L, 0L, 123, new int[]{1, 2}, 494094984L, 86809727L));
  OSHNode node102 = OSHNodeTest.buildOSHNode(new OSMNode(
      102L, 1, 1L, 0L, 123, new int[]{2, 1}, 494094984L, 86809727L));
  OSHNode node104 = OSHNodeTest.buildOSHNode(new OSMNode(
      104L, 1, 1L, 0L, 123, new int[]{2, 4}, 494094984L, 86809727L));

  public OSHWayTest() throws IOException {}

  @Test
  public void testGetNodes() throws IOException {
    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        new OSMWay(123, 1, 3333L, 4444L, 23, new int[]{1, 1, 2, 1},
            new OSMMember[]{
                new OSMMember(102, OSMType.NODE, 0),
                new OSMMember(104, OSMType.NODE, 0)}),
        new OSMWay(123, 3, 3333L, 4444L, 23, new int[]{1, 1, 2, 2},
            new OSMMember[]{
                new OSMMember(100, OSMType.NODE, 0),
                new OSMMember(104, OSMType.NODE, 0)})
    ), Arrays.asList(node100, node102, node104));
    assertNotNull(hway);

    List<OSHNode> nodes = hway.getNodes();
    assertEquals(3, nodes.size());
  }

  @Test
  public void testWithMissingNode() throws IOException {
    OSHWay hway = OSHWayImpl.build(Lists.newArrayList(
        new OSMWay(123, 3, 3333L, 4444L, 23, new int[]{1, 1, 2, 2},
            new OSMMember[]{
                new OSMMember(100, OSMType.NODE, 0),
                new OSMMember(104, OSMType.NODE, 0)}),
        new OSMWay(123, 1, 3333L, 4444L, 23, new int[]{1, 1, 2, 1},
            new OSMMember[]{
                new OSMMember(102, OSMType.NODE, 0),
                new OSMMember(104, OSMType.NODE, 0)})
    ), Arrays.asList(node100, node104));
    assertNotNull(hway);

    List<OSHNode> nodes = hway.getNodes();
    assertEquals(2, nodes.size());

    OSMWay way;
    OSMMember[] members;
    Iterator<OSMWay> itr = hway.getVersions().iterator();
    assertTrue(itr.hasNext());
    way = itr.next();
    members = way.getMembers();
    assertEquals(2, members.length);
    assertEquals(100, members[0].getId());
    assertEquals(104, members[1].getId());

    assertTrue(itr.hasNext());
    way = itr.next();
    members = way.getMembers();
    assertEquals(2, members.length);

    assertEquals(102, members[0].getId());
    assertEquals(104, members[1].getId());
  }

  @Test
  public void testToString() throws IOException {
    OSHWay instance = OSHWayImpl.build(Lists.newArrayList(
        new OSMWay(123, 1, 3333L, 4444L, 23, new int[]{1, 1, 2, 1},
            new OSMMember[]{
                new OSMMember(102, OSMType.NODE, 0),
                new OSMMember(104, OSMType.NODE, 0)}),
        new OSMWay(123, 3, 3333L, 4444L, 23, new int[]{1, 1, 2, 2},
            new OSMMember[]{
                new OSMMember(100, OSMType.NODE, 0),
                new OSMMember(104, OSMType.NODE, 0)})
    ), Arrays.asList(node100, node102, node104));

    String expResult =
        "OSHWay ID:123 Vmax:+3+ Creation:3333 BBox:(8.680973,49.409498),(8.680973,49.409498)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }
}
