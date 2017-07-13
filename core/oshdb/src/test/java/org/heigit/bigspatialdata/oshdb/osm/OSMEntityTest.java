package org.heigit.bigspatialdata.oshdb.osm;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class OSMEntityTest {

  private static final Logger LOG = Logger.getLogger(OSMEntityTest.class.getName());

  @Test
  public void testToString() {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, properties, 1000000000L, 1000000000L);
    String expResult = "NODE: ID:1 V:+1+ TS:1 CS:1 VIS:true USER:1 TAGS:[1, 2] 100.000000:100.000000";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testToGeoJSON_List_TagInterpreter() {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, properties, 1000000000L, 1000000000L);
    List<Pair<? extends OSMEntity, Long>> OSMObjects = new ArrayList<>();
    OSMObjects.add(new ImmutablePair<>(instance, 1L));
    OSMObjects.add(new ImmutablePair<>(instance, 2L));
    TagInterpreter areaDecider = new TagInterpreter(1, 1, null, null, null, 1, 1, 1);
    String expResult = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"id\":1,\"properties\":{\"version\":1,\"changeset\":1,\"timestamp\":1,\"userid\":1,\"1\":2},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}},{\"type\":\"Feature\",\"id\":1,\"properties\":{\"version\":1,\"changeset\":1,\"timestamp\":1,\"userid\":1,\"1\":2},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}}]}";
    String result = OSMEntity.toGeoJSON(OSMObjects, areaDecider);
    assertEquals(expResult, result);
  }

  @Test
  public void testToGeoJSON_long_TagInterpreter() {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, properties, 1000000000L, 1000000000L);
    String expResult = "{\"type\":\"Feature\",\"id\":1,\"properties\":{\"version\":1,\"changeset\":1,\"timestamp\":1,\"userid\":1,\"1\":2},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}}";
    String result = instance.toGeoJSON(1L, new TagInterpreter(1, 1, null, null, null, 1, 1, 1));
    assertEquals(expResult, result);
  }

}
