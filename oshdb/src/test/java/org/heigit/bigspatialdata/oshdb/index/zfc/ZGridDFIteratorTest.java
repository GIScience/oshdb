package org.heigit.bigspatialdata.oshdb.index.zfc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.LongBoundingBox;
import org.junit.Test;

import com.google.common.collect.Lists;

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
    {
    Iterator<Long> itr = grid.iteratorDF(new LongBoundingBox(0,(long)(360*OSMNode.GEOM_PRECISION_TO_LONG), 0 , (long)(180*OSMNode.GEOM_PRECISION_TO_LONG)));
    while(itr.hasNext()){
      final long zid = itr.next();
      final int z = ZGrid.getZoom(zid);
      final long id = ZGrid.getIdWithoutZoom(zid);
      System.out.printf("z:%2d - %3d (%d)%n",z,id,zid);
    }
    }
    
    Iterator<Long> itr = grid.iteratorDF(new LongBoundingBox(0,(long)(360*OSMNode.GEOM_PRECISION_TO_LONG), 0 , (long)(180*OSMNode.GEOM_PRECISION_TO_LONG)));
    order.forEach((id)->{
      assertTrue(itr.hasNext());
      assertEquals(id, itr.next());
    });
    assertTrue(!itr.hasNext());
  }

}
