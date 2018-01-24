package org.heigit.bigspatialdata.oshdb.index.zfc;

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ZGridDFIteratorTest {

  List<Long> order = Lists.newArrayList(
          ZGrid.addZoomToId(0, 0),
          ZGrid.addZoomToId(0, 1),
          ZGrid.addZoomToId(0, 2),
          //            ZGrid.addZoomToId(0, 3),
          //            ZGrid.addZoomToId(1, 3),
          //            ZGrid.addZoomToId(2, 3),
          //            ZGrid.addZoomToId(3, 3),
          ZGrid.addZoomToId(1, 2),
          //            ZGrid.addZoomToId(4, 3),
          //            ZGrid.addZoomToId(5, 3),
          //            ZGrid.addZoomToId(6, 3),
          //            ZGrid.addZoomToId(7, 3),
          ZGrid.addZoomToId(2, 2),
          //            ZGrid.addZoomToId(8, 3),
          //            ZGrid.addZoomToId(9, 3),
          //            ZGrid.addZoomToId(10, 3),
          //            ZGrid.addZoomToId(11, 3),
          ZGrid.addZoomToId(3, 2),
          //            ZGrid.addZoomToId(12, 3),
          //            ZGrid.addZoomToId(13, 3),
          //            ZGrid.addZoomToId(14, 3),
          //            ZGrid.addZoomToId(15, 3),
          ZGrid.addZoomToId(1, 1),
          ZGrid.addZoomToId(4, 2),
          ZGrid.addZoomToId(5, 2),
          ZGrid.addZoomToId(6, 2),
          ZGrid.addZoomToId(7, 2)
  );

  @Test
  public void test() {
    ZGrid grid = new ZGrid(2);

    Iterator<Long> itr = grid.iteratorDF(new OSHDBBoundingBox(0L, 0L, (long) (360 * OSHDB.GEOM_PRECISION_TO_LONG), (long) (180 * OSHDB.GEOM_PRECISION_TO_LONG)));
    order.forEach((id) -> {
      assertTrue(itr.hasNext());
      assertEquals(id, itr.next());
    });
    assertTrue(!itr.hasNext());
  }

}
