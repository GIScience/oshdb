package org.heigit.bigspatialdata.ohsome.ohsomeapi.utils.tagfilter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for the tagfilter package.
 */
public class TagFilterTest {
  private FilterParser parser;
  private TagTranslator tagTranslator;

  @Before
  public void setup() throws SQLException, ClassNotFoundException, OSHDBKeytablesNotFoundException {
    this.tagTranslator = new TagTranslator(
        (new OSHDBH2("./src/test/resources/tagFilterTestKeytables")).getConnection());
    this.parser = new FilterParser(this.tagTranslator);
  }

  @After
  public void teardown() throws SQLException {
    this.tagTranslator.getConnection().close();
  }

  private OSMEntity createTestEntity(String ...keyValues) {
    ArrayList<Integer> tags = new ArrayList<>(keyValues.length);
    for (int i = 0; i < keyValues.length; i += 2) {
      OSHDBTag t = tagTranslator.getOSHDBTagOf(keyValues[i], keyValues[i + 1]);
      tags.add(t.getKey());
      tags.add(t.getValue());
    }
    int[] tagIds = tags.stream().mapToInt(x -> x).toArray();
    return new OSMNode(1, 1, null, 1, 1, tagIds, 0, 0);
  }

  @Test
  public void testTagFilterEquals() {
    FilterExpression expression = parser.parse("highway=residential");
    assertTrue(expression instanceof TagFilterEquals);
    assertTrue(expression.applyOSM(createTestEntity("highway", "residential")));
    assertFalse(expression.applyOSM(createTestEntity("highway", "track")));
    assertFalse(expression.applyOSM(createTestEntity("building", "yes")));
  }

  @Test
  public void testTagFilterEqualsAny() {
    FilterExpression expression = parser.parse("highway=*");
    assertTrue(expression instanceof TagFilterEqualsAny);
    assertTrue(expression.applyOSM(createTestEntity("highway", "residential")));
    assertFalse(expression.applyOSM(createTestEntity("building", "yes")));
  }

  @Test
  public void testTagFilterNotEquals() {
    FilterExpression expression = parser.parse("highway!=residential");
    assertTrue(expression instanceof TagFilterNotEquals);
    assertFalse(expression.applyOSM(createTestEntity("highway", "residential")));
    assertTrue(expression.applyOSM(createTestEntity("highway", "track")));
    assertTrue(expression.applyOSM(createTestEntity("building", "yes")));
  }

  @Test
  public void testTagFilterNotEqualsAny() {
    FilterExpression expression = parser.parse("highway!=*");
    assertTrue(expression instanceof TagFilterNotEqualsAny);
    assertFalse(expression.applyOSM(createTestEntity("highway", "track")));
    assertTrue(expression.applyOSM(createTestEntity("building", "yes")));
  }
}
