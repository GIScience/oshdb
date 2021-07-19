package org.heigit.ohsome.oshdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
      100L, 1, 1L, 0L, 123, new int[]{1, 2}, 494094984, 86809727));
  OSHNode node102 = OSHNodeTest.buildOSHNode(new OSMNode(
      102L, 1, 1L, 0L, 123, new int[]{2, 1}, 494094984, 86809727));
  OSHNode node104 = OSHNodeTest.buildOSHNode(new OSMNode(
      104L, 1, 1L, 0L, 123, new int[]{2, 4}, 494094984, 86809727));

  public OSHWayTest() throws IOException {}

  @Test
  public void testGetNodes() throws IOException, ClassNotFoundException {
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

    var baos = new ByteArrayOutputStream();
    try (var oos = new ObjectOutputStream(baos)) {
      oos.writeObject(hway);
    }
    assertEquals(true, baos.size() > 0);
    var bais = new ByteArrayInputStream(baos.toByteArray());
    try (var ois = new ObjectInputStream(bais)) {
      var newWay = (OSHWay) ois.readObject();

      assertEquals(hway.getId(), newWay.getId());
      assertEquals(Iterables.size(hway.getVersions()), Iterables.size(newWay.getVersions()));
    }
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

  @Test
  public void testHashCodeEquals() throws IOException {
    var expected = OSHWayImpl.build(Lists.newArrayList(
        new OSMWay(123L, 1, 3333L, 4444L, 23, new int[]{},
            new OSMMember[]{})), Arrays.asList());

    var a = OSHWayImpl.build(Lists.newArrayList(
        new OSMWay(123L, 1, 3333L, 4444L, 23, new int[]{},
            new OSMMember[]{})), Arrays.asList());

    var b = OSHWayImpl.build(Lists.newArrayList(
        new OSMWay(444L, 1, 3333L, 4444L, 23, new int[]{},
            new OSMMember[]{})), Arrays.asList());

    assertEquals(expected.hashCode(), a.hashCode());
    assertNotEquals(expected.hashCode(), b.hashCode());

    assertEquals(expected, a);
    assertNotEquals(expected, b);
  }
}
