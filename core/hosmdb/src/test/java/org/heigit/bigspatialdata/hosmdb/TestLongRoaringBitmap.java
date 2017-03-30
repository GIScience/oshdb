package org.heigit.bigspatialdata.hosmdb;

import static org.junit.Assert.*;

import org.heigit.bigspatialdata.hosmdb.util.bitset.LongRoaringBitmap;
import org.junit.Test;

public class TestLongRoaringBitmap {

  @Test
  public void test() {
    
    LongRoaringBitmap lrb = new LongRoaringBitmap();
    
    long l = Integer.MAX_VALUE + 123l;
    
    assertTrue(l > Integer.MAX_VALUE);
    
    lrb.add(l);
    assertTrue(lrb.get(l));
    assertTrue(!lrb.get(l+123));
  }

}
