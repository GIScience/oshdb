package org.heigit.ohsome.oshdb.grid;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.junit.Test;

public class GridOSHWaysTest {

  static OSHNode buildOSHNode(List<OSMNode> versions) {
    try {
      return OSHNodeImpl.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  OSHNode node100 = buildOSHNode(
      Arrays.asList(new OSMNode(100L, 1, 1L, 0L, 123, new int[] {1, 2}, 494094984L, 86809727L)));
  OSHNode node102 = buildOSHNode(
      Arrays.asList(new OSMNode(102L, 1, 1L, 0L, 123, new int[] {2, 1}, 494094984L, 86809727L)));
  OSHNode node104 = buildOSHNode(
      Arrays.asList(new OSMNode(104L, 1, 1L, 0L, 123, new int[] {2, 4}, 494094984L, 86809727L)));

  @Test
  public void testToString() throws IOException {
    List<OSHWay> hosmWays = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      List<OSMWay> versions = new ArrayList<>();
      versions.add(new OSMWay(123, 1, 3333L, 4444L, 23, new int[] {1, 1, 2, 1}, new OSMMember[] {
          new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
      versions.add(new OSMWay(123, 3, 3333L, 4444L, 23, new int[] {1, 1, 2, 2}, new OSMMember[] {
          new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
      hosmWays.add(OSHWayImpl.build(versions, Arrays.asList(node100, node102, node104)));
    }

    GridOSHWays instance = GridOSHWays.compact(2, 2, 100, 100000L, 86000000, 490000000, hosmWays);
    String expResult =
        "Grid-Cell of OSHWays ID:2 Level:2 BBox:(-90.000000,0.000000),(-0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }
}