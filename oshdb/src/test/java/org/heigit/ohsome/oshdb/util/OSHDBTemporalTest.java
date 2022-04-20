package org.heigit.ohsome.oshdb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.heigit.ohsome.oshdb.OSHDBTemporal;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.junit.jupiter.api.Test;

public class OSHDBTemporalTest {
  @Test
  public void testBeforeAfter() {
    OSHDBTemporal t1 = new OSHDBTimestamp(0);
    OSHDBTemporal t2 = new OSHDBTimestamp(1);
    assertTrue(t1.isBefore(t2));
    assertTrue(t2.isAfter(t1));
    assertEquals(0, OSHDBTemporal.compare(t1, t1));
    assertTrue(OSHDBTemporal.compare(t1, t2) < 0);
    assertTrue(OSHDBTemporal.compare(t2, t1) > 0);
  }
}
