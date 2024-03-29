package org.heigit.ohsome.oshdb.osh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.junit.jupiter.api.Test;

class OSHNodeTest {
  private static final int USER_A = 1;
  private static final int[] TAGS_A = new int[] {1, 1};
  private static final int[] LONLAT_A = new int[] {86756350, 494186210};
  private static final int[] LONLAT_B = new int[] {87153340, 494102830};

  @Test
  void testBuildAndSerialize() throws IOException, ClassNotFoundException {
    OSHNode hnode = buildOSHNode(
        OSM.node(123L, 1, 1L, 0L, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]),
        OSM.node(123L, -2, 2L, 0L, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1])
    );

    assertNotNull(hnode);

    List<OSMNode> v = OSHEntities.toList(hnode.getVersions());
    assertNotNull(v);
    assertEquals(2, v.size());

    var baos = new ByteArrayOutputStream();
    try (var oos = new ObjectOutputStream(baos)) {
      oos.writeObject(hnode);
    }
    assertEquals(true, baos.size() > 0);
    var bais = new ByteArrayInputStream(baos.toByteArray());
    try (var ois = new ObjectInputStream(bais)) {
      var newNode = (OSHNode) ois.readObject();

      assertEquals(hnode.getId(), newNode.getId());
      assertEquals(Iterables.size(hnode.getVersions()), Iterables.size(newNode.getVersions()));
    }
  }

  @Test
  void testToString() throws IOException {
    OSHNode instance = buildOSHNode(
        OSM.node(123L, 2, 2L, 0L, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]),
        OSM.node(123L, 1, 1L, 0L, USER_A, TAGS_A, LONLAT_B[0], LONLAT_B[1])
    );

    String expResult =
        "OSHNode ID:123 Vmax:+2+ Creation:1 BBox:(49.410283,8.675635),(49.418621,8.715334)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  void testHashCodeEquals() throws IOException {
    var expected = buildOSHNode(
        OSM.node(123L, 1, 1L, 0L, USER_A, TAGS_A, 0, 0)
    );

    var a = buildOSHNode(
        OSM.node(123L, 1, 1L, 0L, USER_A, TAGS_A, 0, 0)
    );

    var b = buildOSHNode(
        OSM.node(444L, 1, 2L, 0L, USER_A, TAGS_A, 0, 0)
    );

    assertEquals(expected.hashCode(), a.hashCode());
    assertNotEquals(expected.hashCode(), b.hashCode());

    assertEquals(expected, a);
    assertNotEquals(expected, b);
  }

  static OSHNode buildOSHNode(OSMNode... versions) throws IOException {
    return OSHNodeImpl.build(Lists.newArrayList(versions));
  }
}
