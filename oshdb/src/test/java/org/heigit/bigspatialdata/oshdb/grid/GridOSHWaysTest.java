package org.heigit.bigspatialdata.oshdb.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.junit.Test;

public class GridOSHWaysTest {

  static OSHNode buildHOSMNode(List<OSMNode> versions) {
    try {
      return OSHNode.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  OSHNode node100 = buildHOSMNode(
          Arrays.asList(new OSMNode(100l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{1, 2}, 494094984l, 86809727l)));
  OSHNode node102 = buildHOSMNode(
          Arrays.asList(new OSMNode(102l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 1}, 494094984l, 86809727l)));
  OSHNode node104 = buildHOSMNode(
          Arrays.asList(new OSMNode(104l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 4}, 494094984l, 86809727l)));

  @Test
  public void testToString() throws IOException {
    List<OSHWay> hosmWays = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      List<OSMWay> versions = new ArrayList<>();
      versions.add(
              new OSMWay(123, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 1, 2, 1}, new OSMMember[]{new OSMMember(102, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
      versions.add(
              new OSMWay(123, 3, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 1, 2, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)}));
      hosmWays.add(OSHWay.build(versions, Arrays.asList(node100, node102, node104)));
    }

    GridOSHWays instance = GridOSHWays.compact(2, 2, 100, 100000l, 86000000, 490000000, hosmWays);
    String expResult = "Grid-Cell of OSHWays ID:2 Level:2 BBox:(-90.000000,0.000000),(-0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
