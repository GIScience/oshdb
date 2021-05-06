package org.heigit.ohsome.oshdb.grid;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.junit.Test;

public class GridOSHNodesTest {

  //  @Test
  //  public void testOSHCell() {
  //    try {
  //      List<OSHNode> hosmNodes = new ArrayList<>();
  //      for (int i = 0; i < 3; i++) {
  //        List<OSMNode> versions = new ArrayList<>();
  //        versions.add(
  //                new OSMNode(123L + (10 * i), 1, (123001L + (10 * i)), 0L, 123, new int[] {},
  //            86809727L - (1000000 * i), 494094984L - (1000000 * i)));
  //        versions.add(
  //                new OSMNode(123L + (10 * i), 2, (123002L + (10 * i)), 0L, 123, new int[] {},
  //            86809727L - (1000000 * i), 494094984L - (1000000 * i)));
  //        hosmNodes.add(OSHNodeImpl.build(versions));
  //      }
  //      GridOSHNodes cell = GridOSHNodes
  //                            .rebase(123, 2, 100, 100000L, 86000000, 490000000, hosmNodes);
  //      int countOSHNodes = 0;
  //      int countNodes = 0;
  //      Iterator<OSHNode> itr = cell.iterator();
  //      while (itr.hasNext()) {
  //        OSHNode hn = itr.next();
  //        Iterator<OSMNode> itr2 = hn.getVersions().iterator();
  //        while (itr2.hasNext()) {
  //          itr2.next();
  //          countNodes++;
  //        }
  //        countOSHNodes++;
  //      }
  //      // System.out.printf("hnodes:%d nodes:%d\n", countHNodes, countNodes);
  //    } catch (IOException e) {
  //      // TODO Auto-generated catch block
  //      e.printStackTrace();
  //    }
  //  }

  @Test
  public void testToString() throws IOException {
    List<OSHNode> hosmNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      List<OSMNode> versions = new ArrayList<>();
      versions.add(new OSMNode(123L + (10 * i), 1, (123001L + (10 * i)), 0L, 123, new int[] {},
          86809727L - (1000000 * i), 494094984L - (1000000 * i)));
      versions.add(new OSMNode(123L + (10 * i), 2, (123002L + (10 * i)), 0L, 123, new int[] {},
          86809727L - (1000000 * i), 494094984L - (1000000 * i)));
      hosmNodes.add(OSHNodeImpl.build(versions));
    }

    GridOSHNodes instance = GridOSHNodes.rebase(2, 2, 100, 100000L, 86000000, 490000000, hosmNodes);
    String expResult =
        "Grid-Cell of OSHNodes ID:2 Level:2 BBox:(-90.000000,0.000000),(-0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
