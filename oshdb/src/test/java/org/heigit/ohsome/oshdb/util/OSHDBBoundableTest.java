package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.junit.jupiter.api.Test;

class OSHDBBoundableTest {
  private OSHDBBoundable point = OSHDBBoundingBox.bboxOSMCoordinates(0, 0, 0, 0);
  private OSHDBBoundable box = OSHDBBoundingBox.bboxOSMCoordinates(-1, -1, 1, 1);

  @Test
  void testPoint() {
    assertTrue(point.isPoint());
    assertFalse(box.isPoint());
  }

  @Test
  void testValid() {
    assertTrue(point.isValid());
    assertTrue(box.isValid());
    OSHDBBoundable invalid = OSHDBBoundingBox.bboxOSMCoordinates(1, 1, -1, -1);
    assertFalse(invalid.isValid());
  }

  @Test
  void testCovered() {
    assertTrue(point.coveredBy(box));
    assertFalse(point.coveredBy(null));
  }

  @Test
  void testIntersects() {
    assertTrue(point.intersects(box));
    assertFalse(point.intersects(null));
  }

  @Test
  void testIntersection() {
    OSHDBBoundable box2 = OSHDBBoundingBox.bboxOSMCoordinates(0, 0, 2, 2);
    OSHDBBoundable intersection = box2.intersection(box);
    assertEquals(0, intersection.getMinLongitude());
    assertEquals(0, intersection.getMinLatitude());
    assertEquals(1, intersection.getMaxLongitude());
    assertEquals(1, intersection.getMaxLatitude());
  }
}
