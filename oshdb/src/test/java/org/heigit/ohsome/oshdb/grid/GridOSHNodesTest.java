package org.heigit.ohsome.oshdb.grid;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.junit.Test;

public class GridOSHNodesTest {

  @Test
  public void testToString() throws IOException {
    List<OSHNode> hosmNodes = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      List<OSMNode> versions = new ArrayList<>();
      versions.add(new OSMNode(123L + 10 * i, 1, 123001L + 10 * i, 0L, 123, new int[] {},
          86809727 - 1000000 * i, 494094984 - 1000000 * i));
      versions.add(new OSMNode(123L + 10 * i, 2, 123002L + 10 * i, 0L, 123, new int[] {},
          86809727 - 1000000 * i, 494094984 - 1000000 * i));
      hosmNodes.add(OSHNodeImpl.build(versions));
    }

    GridOSHNodes instance = GridOSHNodes.rebase(2, 2, 100, 100000L, 86000000, 490000000, hosmNodes);
    var entities = instance.getEntities();
    assertEquals(hosmNodes.size(), Iterables.size(entities));
  }

}
