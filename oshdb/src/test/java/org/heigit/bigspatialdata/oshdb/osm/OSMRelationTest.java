package org.heigit.bigspatialdata.oshdb.osm;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import static org.heigit.bigspatialdata.oshdb.osh.OSHNodeTest.LONLAT_A;
import static org.heigit.bigspatialdata.oshdb.osh.OSHNodeTest.TAGS_A;
import static org.heigit.bigspatialdata.oshdb.osh.OSHNodeTest.USER_A;
import org.heigit.bigspatialdata.oshdb.util.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class OSMRelationTest {

  public OSMRelationTest() {
  }

  @Test
  public void testGetMembers() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    OSMMember[] expResult = new OSMMember[]{part, part};
    OSMMember[] result = instance.getMembers();
    assertArrayEquals(expResult, result);
  }

  @Test
  public void testCompareTo() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    OSMRelation o = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    assertTrue(instance.compareTo(o) < 0);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    o = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    assertTrue(instance.compareTo(o) == 0);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    o = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    assertTrue(instance.compareTo(o) > 0);
  }

  //-----------------------

  @Test
  public void testGetId() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    long expResult = 1L;
    long result = instance.getId();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetVersion() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    int expResult = 2;
    int result = instance.getVersion();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTimestamp() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    long expResult = 1L;
    long result = instance.getTimestamp();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetChangeset() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    long expResult = 1L;
    long result = instance.getChangeset();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetUserId() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    int expResult = 1;
    int result = instance.getUserId();
    assertEquals(expResult, result);
  }

  @Test
  public void testisVisible() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    boolean expResult = true;
    boolean result = instance.isVisible();
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, -2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    expResult = false;
    result = instance.isVisible();
    assertEquals(expResult, result);
  }

  @Test
  public void testGetTags() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 1, 2, 2}, new OSMMember[]{part, part});
    int[] expResult = new int[]{1, 1, 2, 2};
    int[] result = instance.getTags();
    Assert.assertArrayEquals(expResult, result);
  }

  @Test
  public void testHasTagKey() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, new OSMMember[]{part, part});
    expResult = true;
    result = instance.hasTagKey(1);
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 2, 2, 2, 3, 3}, new OSMMember[]{part, part});
    expResult = false;
    result = instance.hasTagKeyExcluding(1, new int[]{2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 2, 3, 3}, new OSMMember[]{part, part});
    expResult = true;
    result = instance.hasTagKeyExcluding(1, new int[]{2, 3});
    assertEquals(expResult, result);

    part = new OSMMember(1L, OSMType.WAY, 1);
    instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{2, 1, 3, 3}, new OSMMember[]{part, part});
    expResult = false;
    result = instance.hasTagKeyExcluding(1, new int[]{1, 3});
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValue() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 2, 2, 3}, new OSMMember[]{part, part});
    boolean expResult = false;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testHasTagValueII() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, new int[]{1, 1, 2, 3}, new OSMMember[]{part, part});
    boolean expResult = true;
    boolean result = instance.hasTagValue(1, 1);
    assertEquals(expResult, result);
  }

  @Test
  public void testToString() {
    OSMMember part = new OSMMember(1L, OSMType.WAY, 1);
    OSMRelation instance = new OSMRelation(1L, 2, 1L, 1L, 1, new int[]{1, 2}, new OSMMember[]{part, part});
    String expResult = "Relation-> ID:1 V:+2+ TS:1 CS:1 VIS:true UID:1 TAGS:[1, 2] Mem:[T:WAY ID:1 R:1, T:WAY ID:1 R:1]";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testToString_TagTranslator() throws SQLException, ClassNotFoundException {
    int[] tags = {1, 2};
    OSMMember[] member = {new OSMMember(2L, OSMType.WAY, 3), new OSMMember(5L, OSMType.NODE, 5)};
    OSMRelation instance = new OSMRelation(1L, 1, 1L, 1L, 1, tags, member);
    String expResult = "RELATION-> ID:1 V:+1+ TS:1 CS:1 VIS:true UID:1 UName:Alice TAGS:[(highway,track)] Mem:[(T:Way ID:2 R:to),(T:Node ID:5 R:via)]";
    String result = instance.toString(new TagTranslator(DriverManager.getConnection("jdbc:h2:./src/test/resources/keytables", "sa", "")));
    assertEquals(expResult, result);
  }

  @Test
  public void testToGeoJSON_long_TagTranslator_TagInterpreter() throws SQLException, ClassNotFoundException, IOException, ParseException {
    List<OSMNode> versions = new ArrayList<>();
    versions.add(new OSMNode(123l, 1, 0l, 1l, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]));
    OSHNode hnode = OSHNode.build(versions);
    OSMMember part = new OSMMember(1L, OSMType.NODE, 0, hnode);
    OSMRelation instance = new OSMRelation(1L, 1, 0L, 1L, 1, new int[]{1, 2}, new OSMMember[]{part, part});
    TagTranslator tt = new TagTranslator(DriverManager.getConnection("jdbc:h2:./src/test/resources/keytables", "sa", ""));
    String expResult = "{\"type\":\"Feature\",\"id\":\"relation/1@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"relation\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@user\":\"Alice\",\"@uid\":1,\"highway\":\"track\",\"members\":[{\"type\":\"NODE\",\"ref\":1,\"role\":\"outer\"},{\"type\":\"NODE\",\"ref\":1,\"role\":\"outer\"}]},\"geometry\":{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[8.675635,49.418620999999995]},{\"type\":\"Point\",\"coordinates\":[8.675635,49.418620999999995]}]}}";

    String result = instance.toGeoJSON(1L, tt, DefaultTagInterpreter.fromJDBC(DriverManager.getConnection("jdbc:h2:./src/test/resources/keytables", "sa", "")));
    assertEquals(expResult, result);
  }

}
