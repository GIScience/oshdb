package org.heigit.ohsome.oshdb.tool.importer.transform.oshdb;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.junit.Test;

/**
 * General {@link TransformOSHNode} test case.
 */
public class TransformOSHNodeTest {

  @Test
  public void test() throws IOException {
    var baData = new ByteArrayOutputWrapper(1024);
    var baRecord = new ByteArrayOutputWrapper(1024);
    var baAux = new ByteArrayOutputWrapper(1024);
    var nodes = new ArrayList<>(List.of(
        new OSMNode(1, 2, 2000, 20, 32, new int[] {1, 1}, 124, 457),
        new OSMNode(1, 1, 1000, 10, 23, new int[0], 123, 456)));
    var actual = TransformOSHNode.build(baData, baRecord, baAux, nodes, 0, 0, 0, 0);

    assertEquals(nodes.size(), Iterables.size(actual));
    var itrExpected = nodes.iterator();
    var itrActual = actual.iterator();
    while (itrExpected.hasNext()) {
      assertEquals(true, itrActual.hasNext());
      assertEquals(itrExpected.next(), itrActual.next());
    }
    assertEquals(false, itrActual.hasNext());
  }
}
