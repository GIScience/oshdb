package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
    Pair<String, String> tag = new ImmutablePair<>("building", "yes");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Pair<Integer, Integer> expResult = new ImmutablePair<>(1, 0);
    Pair<Integer, Integer> result = instance.tag2Int(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testTag2String() {
    Pair<Integer, Integer> tag = new ImmutablePair<>(1, 2);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Pair<String, String> expResult = new ImmutablePair<>("building", "residential");

    Pair<String, String> result = instance.tag2String(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2Int() {
    String role = "from";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 4;
    Integer result = instance.role2Int(role);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2String() {
    Integer role = 1;
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    String expResult = "inner";
    String result = instance.role2String(role);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2Int() {
    String key = "highway";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 2;
    Integer result = instance.key2Int(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2String() {
    Integer key = 1;
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    String expResult = "building";

    String result = instance.key2String(key);
    assertEquals(expResult, result);
  }

}
