package org.heigit.bigspatialdata.oshdb.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
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

    //connect to the "Big"DB
    TagTranslatorTest.conn = DriverManager.getConnection("jdbc:h2:./src/test/resources/keytables", "sa", "");

  }

  @AfterClass
  public static void breakDownClass() throws SQLException {
    TagTranslatorTest.conn.close();
  }

  public TagTranslatorTest() {
  }

  @Test
  public void testTag2Int() {
    Pair<String, String> tag = new ImmutablePair<>("building", "yes");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Pair<Integer, Integer> expResult = new ImmutablePair<>(0, 0);
    Pair<Integer, Integer> result = instance.tag2Int(tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testTag2String() {
    Pair<Integer, Integer> tag = new ImmutablePair<>(1, 2);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Pair<String, String> expResult = new ImmutablePair<>("highway", "track");
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
  public void testGetAllValues() {
    String key = "surface";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    HashMap<String, Integer> val = new HashMap<>(2);
    val.put("unpaved", 0);
    val.put("asphalt", 1);
    val.put("ground", 2);
    val.put("paved", 3);
    val.put("sand", 4);
    val.put("dirt", 5);
    Pair<Integer, Map<String, Integer>> expResult = new ImmutablePair<>(5, val);
    Pair<Integer, Map<String, Integer>> result = instance.getAllValues(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testUsertoID() {
    String name = "Alice";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 1;
    Integer result = instance.usertoID(name);
    assertEquals(expResult, result);
  }

  @Test
  public void testUsertoStr() {
    Integer uid = 2;
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    String expResult = "Bob";
    String result = instance.usertoStr(uid);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2Int() {
    String key = "highway";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 1;
    Integer result = instance.key2Int(key);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2String() {
    Integer key = 1;
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    String expResult = "highway";
    String result = instance.key2String(key);
    assertEquals(expResult, result);
  }

}
