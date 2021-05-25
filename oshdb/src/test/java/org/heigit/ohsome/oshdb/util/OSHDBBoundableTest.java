package org.heigit.ohsome.oshdb.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.junit.Test;

public class OSHDBBoundableTest {
  private OSHDBBoundable point = OSHDBBoundingBox.bboxOSMCoordinates(0, 0, 0, 0);
  private OSHDBBoundable box = OSHDBBoundingBox.bboxOSMCoordinates(-1, -1, 1, 1);

  @Test
  public void testPoint() {
    assertTrue(point.isPoint());
    assertFalse(box.isPoint());
  }

  @Test
  public void testValid() {
    assertTrue(point.isValid());
    assertTrue(box.isValid());
    OSHDBBoundable invalid = OSHDBBoundingBox.bboxOSMCoordinates(1, 1, -1, -1);
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
    OSHDBBoundable box2 = OSHDBBoundingBox.bboxOSMCoordinates(0, 0, 2, 2);
    OSHDBBoundable intersection = box2.intersection(box);
    assertEquals(0, intersection.getMinLongitude());
    assertEquals(0, intersection.getMinLatitude());
    assertEquals(1, intersection.getMaxLongitude());
    assertEquals(1, intersection.getMaxLatitude());
  }
}
