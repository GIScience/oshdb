package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.OSHDBRole;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
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
  public void testTag2Int() {
    OSMTag tag = new OSMTag("building", "yes");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBTag expResult = new OSHDBTag(1, 0);
    OSHDBTag result = instance.oshdbTagOf(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testTag2String() {
    OSHDBTag tag = new OSHDBTag(1, 2);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMTag expResult = new OSMTag("building", "residential");
    OSMTag result = instance.osmTagOf(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2Int() {
    OSMTagKey key = new OSMTagKey("highway");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBTagKey expResult = new OSHDBTagKey(2);
    OSHDBTagKey result = instance.oshdbTagKeyOf(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2String() {
    OSHDBTagKey key = new OSHDBTagKey(1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMTagKey expResult = new OSMTagKey("building");
    OSMTagKey result = instance.osmTagKeyOf(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2Int() {
    OSMRole role = new OSMRole("from");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSHDBRole expResult = new OSHDBRole(4);
    OSHDBRole result = instance.oshdbRoleOf(role);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2String() {
    OSHDBRole role = new OSHDBRole(1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    OSMRole expResult = new OSMRole("inner");
    OSMRole result = instance.osmRoleOf(role);
    assertEquals(expResult, result);
  }
}
