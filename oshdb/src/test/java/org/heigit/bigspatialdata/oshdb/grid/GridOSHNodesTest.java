package org.heigit.bigspatialdata.oshdb.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.junit.Test;

public class GridOSHNodesTest {

  @Test
  public void testHOSMCell() {

    try {
      List<OSHNode> hosmNodes = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        List<OSMNode> versions = new ArrayList<>();
        versions.add(new OSMNode(123l + 10 * i, 1, new OSHDBTimestamp(123001l + 10 * i), 0l, 123, new int[] {},
            86809727l - 1000000 * i, 494094984l - 1000000 * i));
        versions.add(new OSMNode(123l + 10 * i, 2, new OSHDBTimestamp(123002l + 10 * i), 0l, 123, new int[] {},
            86809727l - 1000000 * i, 494094984l - 1000000 * i));
        hosmNodes.add(OSHNode.build(versions));
      }

      GridOSHNodes cell = GridOSHNodes.rebase(123, 2, 100, 100000l, 86000000, 490000000, hosmNodes);

      int countHNodes = 0;
      int countNodes = 0;
      Iterator<OSHNode> itr = cell.iterator();
      while (itr.hasNext()) {
        OSHNode hn = itr.next();
        Iterator<OSMNode> itr2 = hn.iterator();
        while (itr2.hasNext()) {
          itr2.next();
          countNodes++;
        }

        countHNodes++;
      }

      // System.out.printf("hnodes:%d nodes:%d\n", countHNodes, countNodes);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Test
  public void testToString() throws IOException {
    List<OSHNode> hosmNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      List<OSMNode> versions = new ArrayList<>();
      versions.add(new OSMNode(123l + 10 * i, 1, new OSHDBTimestamp(123001l + 10 * i), 0l, 123, new int[] {},
          86809727l - 1000000 * i, 494094984l - 1000000 * i));
      versions.add(new OSMNode(123l + 10 * i, 2, new OSHDBTimestamp(123002l + 10 * i), 0l, 123, new int[] {},
          86809727l - 1000000 * i, 494094984l - 1000000 * i));
      hosmNodes.add(OSHNode.build(versions));
    }

    GridOSHNodes instance = GridOSHNodes.rebase(2, 2, 100, 100000l, 86000000, 490000000, hosmNodes);
    String expResult =
        "Grid-Cell of OSHNodes ID:2 Level:2 BBox:(-90.000000,0.000000),(-0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
