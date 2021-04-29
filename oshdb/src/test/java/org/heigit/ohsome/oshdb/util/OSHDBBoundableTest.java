package org.heigit.ohsome.oshdb.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.junit.Test;

public class OSHDBBoundableTest {
  private OSHDBBoundable point = new OSHDBBoundingBox(0L, 0L, 0L, 0L);
  private OSHDBBoundable box = new OSHDBBoundingBox(-1L, -1L, 1L, 1L);

  @Test
  public void testPoint() {
    assertTrue(point.isPoint());
    assertFalse(box.isPoint());
  }

  @Test
  public void testValid() {
    assertTrue(point.isValid());
    assertTrue(box.isValid());
    OSHDBBoundable invalid = new OSHDBBoundingBox(1L, 1L, -1L, -1L);
    assertFalse(invalid.isValid());
  }

  @Test
  public void testCovered() {
    assertTrue(point.coveredBy(box));
    assertFalse(point.coveredBy(null));
  }

  @Test
  public void testIntersects() {
    assertTrue(point.intersects(box));
    assertFalse(point.intersects(null));
  }

  @Test
  public void testIntersection() {
    OSHDBBoundable box2 = new OSHDBBoundingBox(0L, 0L, 2L, 2L);
    OSHDBBoundable intersection = box2.intersection(box);
    assertEquals(0, intersection.getMinLonLong());
    assertEquals(0, intersection.getMinLatLong());
    assertEquals(1, intersection.getMaxLonLong());
    assertEquals(1, intersection.getMaxLatLong());
  }
}
