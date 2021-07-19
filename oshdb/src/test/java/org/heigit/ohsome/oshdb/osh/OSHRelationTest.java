package org.heigit.ohsome.oshdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.junit.Test;

public class OSHRelationTest {

  OSHNode node100 = OSHNodeTest.buildOSHNode(new OSMNode(
      100L, 1, 1L, 0L, 123, new int[]{1, 2}, 494094980, 86809720));
  OSHNode node102 = OSHNodeTest.buildOSHNode(new OSMNode(
      102L, 1, 1L, 0L, 123, new int[]{2, 1}, 494094970, 86809730));
  OSHNode node104 = OSHNodeTest.buildOSHNode(new OSMNode(
      104L, 1, 1L, 0L, 123, new int[]{2, 4}, 494094960, 86809740));

  OSHWay way200 = OSHWayImpl.build(Lists.newArrayList(
      new OSMWay(200, 1, 3333L, 4444L, 23, new int[]{1, 2}, new OSMMember[]{
          new OSMMember(100, OSMType.NODE, 0),
          new OSMMember(104, OSMType.NODE, 0)})
  ), List.of(node100, node104));
  OSHWay way202 = OSHWayImpl.build(Lists.newArrayList(
      new OSMWay(202, 1, 3333L, 4444L, 23, new int[]{1, 2}, new OSMMember[]{
          new OSMMember(100, OSMType.NODE, 0),
          new OSMMember(102, OSMType.NODE, 0)})
  ), List.of(node100, node102));

  public OSHRelationTest() throws IOException {}

  @Test
  public void testGetNodes() throws IOException {
    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(100, OSMType.NODE, 0),
            new OSMMember(102, OSMType.NODE, 0),
            new OSMMember(104, OSMType.NODE, 0)})
    ), List.of(node100, node102, node104), Collections.emptyList());

    List<OSHNode> nodes = hrelation.getNodes();
    assertEquals(3, nodes.size());
  }

  @Test
  public void testWithMissingNode() throws IOException {
    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(100, OSMType.NODE, 0),
            new OSMMember(102, OSMType.NODE, 0),
            new OSMMember(104, OSMType.NODE, 0)})
    ), List.of(node100, node104), Collections.emptyList());

    List<OSHNode> nodes = hrelation.getNodes();
    assertEquals(2, nodes.size());

    Iterator<OSMRelation> itr = hrelation.getVersions().iterator();
    assertTrue(itr.hasNext());
    OSMRelation r = itr.next();
    assertNotNull(r);
    OSMMember[] members = r.getMembers();
    assertEquals(3, members.length);

    assertEquals(100, members[0].getId());
    assertNotNull(members[0].getEntity());

    assertEquals(102, members[1].getId());
    assertNull(members[1].getEntity());

    assertEquals(104, members[2].getId());
    assertNotNull(members[2].getEntity());
  }

  @Test
  public void testGetWays() throws IOException {
    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(200, OSMType.WAY, 0),
            new OSMMember(202, OSMType.WAY, 0)})
    ), Collections.emptyList(), List.of(way200, way202),
        200L,
        1000L,
        1000,
        1000
    );

    List<OSHWay> ways = hrelation.getWays();
    assertEquals(2, ways.size());
  }

  @Test
  public void testCompactAndSerialize() throws IOException, ClassNotFoundException {
    OSHRelation hrelation = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(100, OSMType.NODE, 0),
            new OSMMember(102, OSMType.NODE, 0),
            new OSMMember(104, OSMType.NODE, 0),
            new OSMMember(200, OSMType.WAY, 0),
            new OSMMember(202, OSMType.WAY, 0)})
    ), List.of(node100, node102, node104), List.of(way200, way202),
        200L,
        1000L,
        1000,
        1000
    );

    List<OSHNode> nodes = hrelation.getNodes();
    assertEquals(3, nodes.size());

    OSHNode node;
    node = nodes.get(0);
    assertEquals(100, node.getId());
    assertEquals(
        node.getVersions().iterator().next().getLon(),
        node100.getVersions().iterator().next().getLon());

    node = nodes.get(1);
    assertEquals(102, node.getId());
    assertEquals(
        node.getVersions().iterator().next().getLon(),
        node102.getVersions().iterator().next().getLon());

    node = nodes.get(2);
    assertEquals(104, node.getId());
    assertEquals(
        node.getVersions().iterator().next().getLon(),
        node104.getVersions().iterator().next().getLon());

    List<OSHWay> ways = hrelation.getWays();
    assertEquals(2, ways.size());

    OSHWay way = ways.get(0);
    assertEquals(200, way.getId());
    assertEquals(
        way.getNodes().get(0).getVersions().iterator().next().getLon(),
        way200.getNodes().get(0).getVersions().iterator().next().getLon());

    var baos = new ByteArrayOutputStream();
    try (var oos = new ObjectOutputStream(baos)) {
      oos.writeObject(hrelation);
    }
    assertEquals(true, baos.size() > 0);
    var bais = new ByteArrayInputStream(baos.toByteArray());
    try (var ois = new ObjectInputStream(bais)) {
      var newRelation = (OSHRelation) ois.readObject();

      assertEquals(hrelation.getId(), newRelation.getId());
      assertEquals(Iterables.size(hrelation.getVersions()),
          Iterables.size(newRelation.getVersions()));
    }
  }

  @Test
  public void testToString() throws IOException {
    OSHRelation instance = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[]{}, new OSMMember[]{
            new OSMMember(100, OSMType.NODE, 0),
            new OSMMember(102, OSMType.NODE, 0),
            new OSMMember(104, OSMType.NODE, 0)})
    ), List.of(node100, node102, node104), Collections.emptyList());
    String expResult =
        "OSHRelation ID:300 Vmax:+1+ Creation:3333 BBox:(8.680972,49.409496),(8.680974,49.409498)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testHashCodeEquals() throws IOException {
    var expected = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(123L, 1, 3333L, 4444L, 23, new int[]{},
            new OSMMember[]{})), Collections.emptyList(), Collections.emptyList());

    var a = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(123L, 1, 3333L, 4444L, 23, new int[]{},
            new OSMMember[]{})), Collections.emptyList(), Collections.emptyList());

    var b = OSHRelationImpl.build(Lists.newArrayList(
        new OSMRelation(444L, 1, 3333L, 4444L, 23, new int[]{},
            new OSMMember[]{})), Collections.emptyList(), Collections.emptyList());

    assertEquals(expected.hashCode(), a.hashCode());
    assertNotEquals(expected.hashCode(), b.hashCode());

    assertEquals(expected, a);
    assertNotEquals(expected, b);
  }
}
