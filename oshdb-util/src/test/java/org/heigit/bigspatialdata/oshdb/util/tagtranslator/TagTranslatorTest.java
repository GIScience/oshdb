package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class TagTranslatorTest {
  private static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException {
    // load H2-support
    Class.forName("org.h2.Driver");

    // connect to the "Big"DB
    TagTranslatorTest.conn =
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data;ACCESS_MODE_DATA=r", "sa", "");

  }

  @AfterClass
  public static void breakDownClass() throws SQLException {
    TagTranslatorTest.conn.close();
  }

  public TagTranslatorTest() {}

  @Test
  public void testTag2Int() throws OSHDBKeytablesNotFoundException {
    OSMTag tag = new OSMTag("building", "yes");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBTag expResult = new OSHDBTag(1, 0);
    OSHDBTag result = instance.getOSHDBTagOf(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testTag2String() throws OSHDBKeytablesNotFoundException {
    OSHDBTag tag = new OSHDBTag(1, 2);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMTag expResult = new OSMTag("building", "residential");
    OSMTag result = instance.getOSMTagOf(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2Int() throws OSHDBKeytablesNotFoundException {
    OSMTagKey key = new OSMTagKey("highway");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBTagKey expResult = new OSHDBTagKey(2);
    OSHDBTagKey result = instance.getOSHDBTagKeyOf(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2String() throws OSHDBKeytablesNotFoundException {
    OSHDBTagKey key = new OSHDBTagKey(1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMTagKey expResult = new OSMTagKey("building");
    OSMTagKey result = instance.getOSMTagKeyOf(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2Int() throws OSHDBKeytablesNotFoundException {
    OSMRole role = new OSMRole("from");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBRole expResult = new OSHDBRole(4);
    OSHDBRole result = instance.getOSHDBRoleOf(role);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2String() throws OSHDBKeytablesNotFoundException {
    OSHDBRole role = new OSHDBRole(1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMRole expResult = new OSMRole("inner");
    OSMRole result = instance.getOSMRoleOf(role);
    assertEquals(expResult, result);
  }
}
