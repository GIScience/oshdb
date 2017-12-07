package org.heigit.bigspatialdata.oshdb.util.bitset;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 */
public class LongRoaringBitmapTest {

  public LongRoaringBitmapTest() {
  }

  @Test
  public void test() {

    LongRoaringBitmap lrb = new LongRoaringBitmap();

    long l = Integer.MAX_VALUE + 123l;

    assertTrue(l > Integer.MAX_VALUE);

    lrb.add(l);
    assertTrue(lrb.get(l));
    assertTrue(!lrb.get(l + 123));
  }

}
