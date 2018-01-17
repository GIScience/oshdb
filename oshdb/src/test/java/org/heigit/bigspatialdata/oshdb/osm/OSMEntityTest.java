package org.heigit.bigspatialdata.oshdb.osm;

import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.junit.Test;

public class OSMEntityTest {

  @Test
  public void testToString() {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, new OSHDBTimestamp(1L), 1L, 1, properties, 1000000000L, 1000000000L);
    String expResult =
        "NODE: ID:1 V:+1+ TS:1 CS:1 VIS:true UID:1 TAGS:[1, 2] 100.000000:100.000000";
    String result = instance.toString();
    assertEquals(expResult, result);
  }


}
