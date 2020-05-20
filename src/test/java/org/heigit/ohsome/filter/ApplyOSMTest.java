package org.heigit.ohsome.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Test class for the ohsome-filter package.
 *
 * <p>Tests the parsing of filters and the application to OSM entities.</p>
 */
public class ApplyOSMTest extends FilterTest {
  @Test
  public void testTagFilterEquals() {
    FilterExpression expression = parser.parse("highway=residential");
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestEntityNode("highway", "track")));
    assertFalse(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterEqualsAny() {
    FilterExpression expression = parser.parse("highway=*");
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterNotEquals() {
    FilterExpression expression = parser.parse("highway!=residential");
    assertFalse(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterNotEqualsAny() {
    FilterExpression expression = parser.parse("highway!=*");
    assertFalse(expression.applyOSM(createTestEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTypeFilter() {
    assertTrue(parser.parse("type:node").applyOSM(createTestEntityNode()));
    assertFalse(parser.parse("type:way").applyOSM(createTestEntityNode()));
  }

  @Test
  public void testAndOperator() {
    FilterExpression expression = parser.parse("highway=residential and name=*");
    assertTrue(expression.applyOSM(createTestEntityNode(
        "highway", "residential",
        "name", "FIXME"
    )));
    assertFalse(expression.applyOSM(createTestEntityNode(
        "highway", "residential"
    )));
    assertFalse(expression.applyOSM(createTestEntityNode(
        "name", "FIXME"
    )));
    assertFalse(expression.applyOSM(createTestEntityNode()));
  }

  @Test
  public void testOrOperator() {
    FilterExpression expression = parser.parse("highway=residential or name=*");
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestEntityNode("name", "FIXME")));
    assertFalse(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSM(createTestEntityNode()));
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {})));
    assertFalse(expression.applyOSM(createTestEntityRelation()));
  }

  @Test
  public void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    assertTrue(expression.applyOSM(createTestEntityWay(new long[] {})));
    assertTrue(expression.applyOSM(createTestEntityWay(new long[] {1,2,3,4,1})));
    assertFalse(expression.applyOSM(createTestEntityNode()));
    assertFalse(expression.applyOSM(createTestEntityRelation()));
  }

  @Test
  public void testGeometryTypeFilterPolygon() {
    FilterExpression expression = parser.parse("geometry:polygon");
    assertTrue(expression.applyOSM(createTestEntityWay(new long[] {1,2,3,1})));
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {1,2,3,4})));
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {1,2,1})));
    assertTrue(expression.applyOSM(createTestEntityRelation("type", "multipolygon")));
    assertFalse(expression.applyOSM(createTestEntityRelation()));
    assertFalse(expression.applyOSM(createTestEntityNode()));
  }

  @Test
  public void testGeometryTypeFilterOther() {
    FilterExpression expression = parser.parse("geometry:other");
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {})));
    assertFalse(expression.applyOSM(createTestEntityNode()));
    assertTrue(expression.applyOSM(createTestEntityRelation()));
  }
}
