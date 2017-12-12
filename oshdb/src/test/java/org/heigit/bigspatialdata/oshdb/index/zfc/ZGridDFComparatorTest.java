package org.heigit.bigspatialdata.oshdb.index.zfc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.collect.Lists;

public class ZGridDFComparatorTest {
  
  private List<Long> randomList() {
    final Random rnd = new Random(68307L);
    final List<Long> list = Lists.newArrayList(ZGrid.addZoomToId(0, 0), ZGrid.addZoomToId(0, 1),
        ZGrid.addZoomToId(0, 2), ZGrid.addZoomToId(1, 2), ZGrid.addZoomToId(2, 2), ZGrid.addZoomToId(3, 2),
        ZGrid.addZoomToId(1, 1), ZGrid.addZoomToId(4, 2), ZGrid.addZoomToId(5, 2), ZGrid.addZoomToId(6, 2),
        ZGrid.addZoomToId(7, 2));
    Collections.shuffle(list, rnd);
    return list;
  }

  @Test
  public void testDFComparatorParentFirst(){
    final List<Long> expected = Lists.newArrayList(
      ZGrid.addZoomToId(0, 0),
        ZGrid.addZoomToId(0, 1),
          ZGrid.addZoomToId(0, 2),
          ZGrid.addZoomToId(1, 2),
          ZGrid.addZoomToId(2, 2),
          ZGrid.addZoomToId(3, 2),
        ZGrid.addZoomToId(1, 1),
          ZGrid.addZoomToId(4, 2),
          ZGrid.addZoomToId(5, 2),
          ZGrid.addZoomToId(6, 2),
          ZGrid.addZoomToId(7, 2)
      );
    
   final List<Long> actual= randomList();
   actual.sort(ZGrid.ORDER_DFS_TOP_DOWN);
   
   Iterator<Long> itr = actual.listIterator();
   //System.out.println("Bubbling");
   expected.forEach(id -> {
     assertTrue(itr.hasNext());
     final Long test = itr.next();
    // System.out.printf("expected %d:%d = actual %d:%d%n",ZGrid.getIdWithoutZoom(id),ZGrid.getZoom(test),ZGrid.getIdWithoutZoom(test),ZGrid.getIdWithoutZoom(test)); 
     assertEquals(id.longValue(), test.longValue());
   });
   assertFalse(itr.hasNext());
  }

  @Test
  public void testDFComparatorBottomUp() {
    final List<Long> expected = Lists.newArrayList(
            ZGrid.addZoomToId(0, 2), 
            ZGrid.addZoomToId(1, 2),
            ZGrid.addZoomToId(2, 2), 
            ZGrid.addZoomToId(3, 2), 
         ZGrid.addZoomToId(0, 1), 
           ZGrid.addZoomToId(4, 2),
           ZGrid.addZoomToId(5, 2), 
           ZGrid.addZoomToId(6, 2), 
           ZGrid.addZoomToId(7, 2), 
         ZGrid.addZoomToId(1, 1),
       ZGrid.addZoomToId(0, 0)
      );

    final List<Long> actual = randomList();
    actual.sort(ZGrid.ORDER_DFS_BOTTOM_UP);

    Iterator<Long> itr = actual.listIterator();
    // System.out.println("Capturing");
    expected.forEach(id -> {
      assertTrue(itr.hasNext());
      final Long test = itr.next();
      // System.out.printf("expected %d:%d = actual
      // %d:%d%n",ZGrid.getIdWithoutZoom(id),ZGrid.getZoom(test),ZGrid.getIdWithoutZoom(test),ZGrid.getIdWithoutZoom(test));
      assertEquals(id.longValue(), test.longValue());
    });
    assertFalse(itr.hasNext());
  }

}
