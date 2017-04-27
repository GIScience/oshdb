package org.heigit.bigspatialdata.oshdb.util.bitset;

import java.util.logging.Logger;
import static org.junit.Assert.*;

import org.heigit.bigspatialdata.oshdb.util.bitset.LongRoaringBitmap;
import org.junit.Test;

/**
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 */
public class LongRoaringBitmapTest {

  private static final Logger LOG = Logger.getLogger(LongRoaringBitmapTest.class.getName());

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
