package org.heigit.ohsome.oshdb.util.tagtranslator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Map;
import org.h2.jdbcx.JdbcConnectionPool;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public abstract class AbstractTagTranslatorTest {

  protected static JdbcConnectionPool source;

  /**
   * Initialize tests by loading the H2 driver and open a connection via jdbc.
   */
  @BeforeAll
  static void setUpClass() {
    source = JdbcConnectionPool.create("jdbc:h2:../data/test-data;ACCESS_MODE_DATA=r", "sa", "");
  }

  @AfterAll
  static void breakDownClass() {
    source.dispose();
  }

  abstract TagTranslator getTranslator();

  @Test
  void testTag2Int() throws OSHDBKeytablesNotFoundException {
    OSMTag tag = new OSMTag("building", "yes");
    TagTranslator instance = getTranslator();
    OSHDBTag expResult = new OSHDBTag(1, 0);
    OSHDBTag result = instance.getOSHDBTagOf(tag).get();
    assertEquals(expResult, result);
  }

  @Test
  void testTags2Int() throws OSHDBKeytablesNotFoundException {
    var tags = Map.of(
        new OSMTag("building", "yes"), new OSHDBTag(1, 0),
        new OSMTag("building", "residential"), new OSHDBTag(1, 2),
        new OSMTag("highway", "primary"), new OSHDBTag(2, 7));

    TagTranslator instance = getTranslator();
    var result = instance.getOSHDBTagOf(tags.keySet());
    assertEquals(tags.size(), result.size());
    for (var entry : tags.entrySet()) {
      var tag = result.get(entry.getKey());
      assertEquals(entry.getValue(), tag);
    }
  }

  @Test
  void testTag2String() throws OSHDBKeytablesNotFoundException {
    OSHDBTag tag = new OSHDBTag(1, 2);
    OSMTag expResult = new OSMTag("building", "residential");
    TagTranslator instance = getTranslator();
    OSMTag result = instance.lookupTag(tag);
    assertEquals(expResult, result);
  }

  @Test
  void testTags2String() {
    var tags = Map.of(
        new OSHDBTag(1, 0), new OSMTag("building", "yes"),
        new OSHDBTag(1, 2), new OSMTag("building", "residential"),
        new OSHDBTag(2, 7), new OSMTag("highway", "primary"));
    TagTranslator instance = getTranslator();
    var result = instance.lookupTag(tags.keySet());
    assertEquals(tags.size(), result.size());
    for (var entry : tags.entrySet()) {
      var tag = result.get(entry.getKey());
      assertEquals(entry.getValue(), tag);
    }
  }

  @Test
  void testOSMEntityTag2String() {
    var osm = OSM.node(123, 1, 1000L, 100L, 1, new int[]{1, 0, 2, 7}, 0, 0);
    TagTranslator instance = getTranslator();
    var tags = instance.lookupTag(osm.getTags());
    assertEquals(2, tags.size());
    assertEquals(new OSMTag("building", "yes"), tags.get(new OSHDBTag(1, 0)));
    assertEquals(new OSMTag("highway", "primary"), tags.get(new OSHDBTag(2, 7)));
  }

  @Test
  void testKey2Int() throws OSHDBKeytablesNotFoundException {
    OSMTagKey key = new OSMTagKey("highway");
    TagTranslator instance = getTranslator();
    OSHDBTagKey expResult = new OSHDBTagKey(2);
    OSHDBTagKey result = instance.getOSHDBTagKeyOf(key).get();
    assertEquals(expResult, result);
  }

  @Test
  void testRole2Int() throws OSHDBKeytablesNotFoundException {
    OSMRole role = new OSMRole("from");
    TagTranslator instance = getTranslator();
    OSHDBRole expResult = OSHDBRole.of(4);
    OSHDBRole result = instance.getOSHDBRoleOf(role).get();
    assertEquals(expResult, result);
  }

  @Test
  void testRole2String() throws OSHDBKeytablesNotFoundException {
    OSHDBRole role = OSHDBRole.of(1);
    TagTranslator instance = getTranslator();
    OSMRole expResult = new OSMRole("inner");
    OSMRole result = instance.lookupRole(role);
    assertEquals(expResult, result);
  }

  @Test
  void testKeysIdentity() {
    TagTranslator instance = getTranslator();
    var tags = new ArrayList<OSMTag>(10);
    for (var i = 0; i < 10; i++) {
      tags.add(instance.lookupTag(new OSHDBTag(1, i)));
    }
    for (var i = 1; i < tags.size(); i++) {
      assertTrue(tags.get(i - 1).getKey() == tags.get(i).getKey());
    }
  }
}
