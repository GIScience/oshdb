package org.heigit.bigspatialdata.oshdb.utils;

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

  private static final Logger LOG = Logger.getLogger(TagTranslatorTest.class.getName());
  private static Connection conn;

  @BeforeClass
  public static void setUpClass() throws ClassNotFoundException, SQLException {
    // load H2-support
    Class.forName("org.h2.Driver");

    //connect to the "Big"DB
    TagTranslatorTest.conn = DriverManager.getConnection("jdbc:h2:./src/test/resources/heidelberg-ccbysa", "sa", "");

  }

  @AfterClass
  public static void breakDownClass() throws SQLException {
    TagTranslatorTest.conn.close();
  }

  public TagTranslatorTest() {
  }

  @Test
  public void testTag2Int() {
    Pair<String, String> Tag = new ImmutablePair<>("building", "yes");
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Pair<Integer, Integer> expResult = new ImmutablePair<>(2, 0);
    Pair<Integer, Integer> result = instance.tag2Int(Tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testTag2String() {
    Pair<Integer, Integer> Tag = new ImmutablePair<>(2, 1);
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Pair<String, String> expResult = new ImmutablePair<>("building", "residential");
    Pair<String, String> result = instance.tag2String(Tag);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2Int() {
    String Role = "from";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 1;
    Integer result = instance.role2Int(Role);
    assertEquals(expResult, result);
  }

  @Test
  public void testRole2String() {
    Integer Role = 2;
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    String expResult = "to";
    String result = instance.role2String(Role);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetAllValues() {
    String Key = "ruins";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    HashMap<String, Integer> val = new HashMap<>(2);
    val.put("yes", 0);
    val.put("monastery", 1);
    Pair<Integer, Map<String, Integer>> expResult = new ImmutablePair<>(899, val);
    Pair<Integer, Map<String, Integer>> result = instance.getAllValues(Key);
    assertEquals(expResult, result);
  }

  @Test
  public void testUsertoID() {
    String Name = "FrankM";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 46;
    Integer result = instance.usertoID(Name);
    assertEquals(expResult, result);
  }

  @Test
  public void testUsertoStr() {
    Integer OSHDbID = 165;
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    String expResult = "Richard";
    String result = instance.usertoStr(OSHDbID);
    assertEquals(expResult, result);
  }

  @Test
  public void testKey2Int() {
    String Key = "highway";
    TagTranslator instance = new TagTranslator(TagTranslatorTest.conn);
    Integer expResult = 1;
    Integer result = instance.key2Int(Key);
    assertEquals(expResult, result);
  }

}
