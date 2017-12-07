package org.heigit.bigspatialdata.oshdb.osm;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class OSMEntityTest {

  @Test
  public void testToString() {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, properties, 1000000000L, 1000000000L);
    String expResult = "NODE: ID:1 V:+1+ TS:1 CS:1 VIS:true UID:1 TAGS:[1, 2] 100.000000:100.000000";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testToString_TagTranlator() throws SQLException, ClassNotFoundException {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, 1L, 1L, 1, properties, 1000000000L, 1000000000L);
    String expResult = "NODE: ID:1 V:+1+ TS:1 CS:1 VIS:true UID:1 UName:Alice TAGS:[(highway,track)] 100.000000:100.000000";
    String result = instance.toString(new TagTranslator(DriverManager.getConnection("jdbc:h2:./src/test/resources/keytables", "sa", "")));
    assertEquals(expResult, result);
  }

  @Test
  public void testToGeoJSON_List_TagInterpreter() throws SQLException, ClassNotFoundException {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, 1415538449L, 1L, 1, properties, 1000000000L, 1000000000L);
    List<Pair<? extends OSMEntity, Long>> OSMObjects = new ArrayList<>();
    OSMObjects.add(new ImmutablePair<>(instance, 1L));
    OSMObjects.add(new ImmutablePair<>(instance, 2L));
    TagInterpreter areaDecider = new TagInterpreter(1, 1, null, null, null, 1, 1, 1);
    TagTranslator tt = new TagTranslator(DriverManager.getConnection("jdbc:h2:./src/test/resources/keytables", "sa", ""));
    String expResult = "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"id\":\"node/1@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"node\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"2014-11-09T13:07:29Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@user\":\"Alice\",\"@uid\":1,\"highway\":\"track\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}},{\"type\":\"Feature\",\"id\":\"node/1@1970-01-01T00:00:02Z\",\"properties\":{\"@type\":\"node\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"2014-11-09T13:07:29Z\",\"@geomtimestamp\":\"1970-01-01T00:00:02Z\",\"@user\":\"Alice\",\"@uid\":1,\"highway\":\"track\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}}]}";
    String result = OSMEntity.toGeoJSON(OSMObjects, tt, areaDecider);
    assertEquals(expResult, result);
  }

}
