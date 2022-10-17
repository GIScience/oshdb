package org.heigit.ohsome.oshdb.util.tagtranslator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.sql.SQLException;
import org.h2.jdbcx.JdbcConnectionPool;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link DefaultTagTranslator} class.
 */
class TagTranslatorTest {
  private static JdbcConnectionPool source;

  /**
   * Initialize tests by loading the H2 driver and open a connection via jdbc.
   *
   * @throws ClassNotFoundException gets thrown if H2 driver class cannot be found
   * @throws SQLException is thrown if the connection fails
   */
  @BeforeAll
  static void setUpClass() throws ClassNotFoundException, SQLException {
    source = JdbcConnectionPool.create("jdbc:h2:../data/test-data;ACCESS_MODE_DATA=r", "sa", "");
  }

  @AfterAll
  static void breakDownClass() throws SQLException {
    source.dispose();
  }

  TagTranslatorTest() {}

  @Test
  void testTag2Int() throws OSHDBKeytablesNotFoundException, SQLException {
    OSMTag tag = new OSMTag("building", "yes");
    try (var instance = new DefaultTagTranslator(source)){
      OSHDBTag expResult = new OSHDBTag(1, 0);
      OSHDBTag result = instance.getOSHDBTagOf(tag);
      assertEquals(expResult, result);
    }
  }

  @Test
  void testTag2String() throws OSHDBKeytablesNotFoundException, SQLException {
    OSHDBTag tag = new OSHDBTag(1, 2);
    try (var instance = new DefaultTagTranslator(source)){
      OSMTag expResult = new OSMTag("building", "residential");
      OSMTag result = instance.lookupTag(tag);
      assertEquals(expResult, result);
    }
  }

  @Test
  void testKey2Int() throws OSHDBKeytablesNotFoundException, SQLException {
    OSMTagKey key = new OSMTagKey("highway");
    try (var instance = new DefaultTagTranslator(source)) {
      OSHDBTagKey expResult = new OSHDBTagKey(2);
      OSHDBTagKey result = instance.getOSHDBTagKeyOf(key);
      assertEquals(expResult, result);
    }
  }

  @Test
  void testRole2Int() throws OSHDBKeytablesNotFoundException, SQLException {
    OSMRole role = new OSMRole("from");
    try (var instance = new DefaultTagTranslator(source)){
      OSHDBRole expResult = OSHDBRole.of(4);
      OSHDBRole result = instance.getOSHDBRoleOf(role);
      assertEquals(expResult, result);
    }
  }

  @Test
  void testRole2String() throws OSHDBKeytablesNotFoundException, SQLException {
    OSHDBRole role = OSHDBRole.of(1);
    try (var instance = new DefaultTagTranslator(source)){
      OSMRole expResult = new OSMRole("inner");
      OSMRole result = instance.lookupRole(role);
      assertEquals(expResult, result);
    }
  }
}
