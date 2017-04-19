package org.heigit.bigspatialdata.hosmdb.grid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMEntity;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.junit.Test;

public class TestHOSMCellNodes {

  @Test
  public void testHOSMCell() {



    try {
      List<HOSMNode> hosmNodes = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        List<OSMNode> versions = new ArrayList<>();
        versions.add(new OSMNode(123l + 10 * i, 1, 123001l + 10 * i, 0l, 123, new int[] {},
            86809727l - 1000000 * i, 494094984l - 1000000 * i));
        versions.add(new OSMNode(123l + 10 * i, 2, 123002l + 10 * i, 0l, 123, new int[] {},
            86809727l - 1000000 * i, 494094984l - 1000000 * i));
        hosmNodes.add(HOSMNode.build(versions));
      }


      HOSMCellNodes cell =HOSMCellNodes.rebase(123, 2,100, 100000l, 86000000, 490000000, hosmNodes);

      System.out.println("Hallo");

      int countHNodes = 0;
      int countNodes = 0;
      Iterator<HOSMNode> itr = cell.iterator();
      while (itr.hasNext()) {
        HOSMNode hn = itr.next();
        Iterator<OSMNode> itr2 = hn.iterator();
        while (itr2.hasNext()) {
          itr2.next();
          countNodes++;
        }

        countHNodes++;
      }

      System.out.printf("hnodes:%d nodes:%d\n", countHNodes, countNodes);

    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


}
