package org.heigit.ohsome.oshdb.osh;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.junit.Test;

public class OSHNodeTest {
  private static final int USER_A = 1;
  private static final int[] TAGS_A = new int[] {1, 1};
  private static final long[] LONLAT_A = new long[] {86756350L, 494186210L};
  private static final long[] LONLAT_B = new long[] {87153340L, 494102830L};

  @Test
  public void testBuild() throws IOException {
    OSHNode hnode = buildOSHNode(
        new OSMNode(123L, 1, 1L, 0L, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]),
        new OSMNode(123L, -2, 2L, 0L, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1])
    );

    assertNotNull(hnode);

    List<OSMNode> v = OSHEntities.toList(hnode.getVersions());
    assertNotNull(v);
    assertEquals(2, v.size());
  }

  /*
  @Test public void testCompact() { fail("Not yet implemented"); }
  @Test
  public void testRebase() throws IOException {

    long baseLongitude = 85341796875L / 100;
    long baseLatitude = 27597656250L / 100;

    List<OSMNode> versions = new ArrayList<>();
    // NODE: ID:3718143950 V:+2+ TS:1480304071000 CS:43996323 VIS:true USER:4803525 TAGS:[]
    // 85391383800:27676689900
    // NODE: ID:3718143950 V:+1+ TS:1440747974000 CS:33637224 VIS:true USER:3191558 TAGS:[]
    // 85391416000:27676640000

    // NODE: ID:3718143950 V:+2+ TS:1480304071000 CS:43996323 VIS:true USER:4803525 TAGS:[]
    // 85391383800:27676689900
    // NODE: ID:3718143950 V:+1+ TS:1440747974000 CS:33637224 VIS:true USER:3191558 TAGS:[]
    // 49619125:78983750
    versions.add(new OSMNode(3718143950L, 2, 1480304071000L / 1000,
        43996323L, 4803525, new int[0], 85391383800L / 100, 27676689900L / 100));
    versions.add(new OSMNode(3718143950L, 1, 1440747974000L / 1000,
        33637224, 3191558, new int[0], 85391416000L / 100, 27676640000L / 100));

    OSHNode hosm = OSHNodeImpl.build(versions);

    // System.out.println("Datasize:" + hosm.getData().length);
    //
    // hosm = hosm.rebase(0, 0, baseLongitude, baseLatitude); System.out.println("Datasize:" +
    // hosm.getData().length); for (OSMNode osm : hosm) { System.out.println(osm); }
    //
    // todo: actually assert something in this test
  }*/

  @Test
  public void testToString() throws IOException {
    OSHNode instance = buildOSHNode(
        new OSMNode(123L, 2, 2L, 0L, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]),
        new OSMNode(123L, 1, 1L, 0L, USER_A, TAGS_A, LONLAT_B[0], LONLAT_B[1])
    );

    String expResult =
        "OSHNode ID:123 Vmax:+2+ Creation:1 BBox:(49.410283,8.675635),(49.418621,8.715334)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  static OSHNode buildOSHNode(OSMNode... versions) throws IOException {
    return OSHNodeImpl.build(Lists.newArrayList(versions));
  }
}
