package org.heigit.ohsome.oshdb.util.tagtranslator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.heigit.ohsome.oshdb.OSHDBRole;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link TagTranslator} class.
 */
class TagTranslatorTest {
  private static Connection conn;

  /**
   * Initialize tests by loading the H2 driver and open a connection via jdbc.
   *
   * @throws ClassNotFoundException gets thrown if H2 driver class cannot be found
   * @throws SQLException is thrown if the connection fails
   */
  @BeforeAll
  static void setUpClass() throws ClassNotFoundException, SQLException {
    // load H2-support
    Class.forName("org.h2.Driver");

    // connect to the test data DB
    TagTranslatorTest.conn =
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data;ACCESS_MODE_DATA=r",
            "sa", "");
  }

  @AfterAll
  static void breakDownClass() throws SQLException {
    TagTranslatorTest.conn.close();
  }

  TagTranslatorTest() {}

  @Test
  void testTag2Int() throws OSHDBKeytablesNotFoundException {
    OSMTag tag = new OSMTag("building", "yes");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBTag expResult = new OSHDBTag(1, 0);
    OSHDBTag result = instance.getOSHDBTagOf(tag);
    assertEquals(expResult, result);
  }

  @Test
  void testTag2String() throws OSHDBKeytablesNotFoundException {
    OSHDBTag tag = new OSHDBTag(1, 2);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMTag expResult = new OSMTag("building", "residential");
    OSMTag result = instance.getOSMTagOf(tag);
    assertEquals(expResult, result);
  }

  @Test
  void testKey2Int() throws OSHDBKeytablesNotFoundException {
    OSMTagKey key = new OSMTagKey("highway");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBTagKey expResult = new OSHDBTagKey(2);
    OSHDBTagKey result = instance.getOSHDBTagKeyOf(key);
    assertEquals(expResult, result);
  }

  @Test
  void testKey2String() throws OSHDBKeytablesNotFoundException {
    OSHDBTagKey key = new OSHDBTagKey(1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMTagKey expResult = new OSMTagKey("building");
    OSMTagKey result = instance.getOSMTagKeyOf(key);
    assertEquals(expResult, result);
  }

  @Test
  void testRole2Int() throws OSHDBKeytablesNotFoundException {
    OSMRole role = new OSMRole("from");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBRole expResult = OSHDBRole.of(4);
    OSHDBRole result = instance.getOSHDBRoleOf(role);
    assertEquals(expResult, result);
  }

  @Test
  void testRole2String() throws OSHDBKeytablesNotFoundException {
    OSHDBRole role = OSHDBRole.of(1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMRole expResult = new OSMRole("inner");
    OSMRole result = instance.getOSMRoleOf(role);
    assertEquals(expResult, result);
  }
}
