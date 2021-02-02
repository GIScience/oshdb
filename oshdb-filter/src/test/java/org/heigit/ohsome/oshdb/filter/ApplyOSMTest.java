package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.junit.Test;

/**
 * Tests the parsing of filters and the application to OSM entities.
 */
public class ApplyOSMTest extends FilterTest {
  @Test
  public void testTagFilterEquals() {
    FilterExpression expression = parser.parse("highway=residential");
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("highway", "track")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterEqualsAny() {
    FilterExpression expression = parser.parse("highway=*");
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterNotEquals() {
    FilterExpression expression = parser.parse("highway!=residential");
    assertFalse(expression.applyOSM(createTestOSMEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterNotEqualsAny() {
    FilterExpression expression = parser.parse("highway!=*");
    assertFalse(expression.applyOSM(createTestOSMEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("highway in (residential, track)");
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "track")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("name", "FIXME")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode()));
    assertFalse(expression.applyOSM(createTestOSMEntityNode(
        "building", "yes",
        "highway", "primary",
        "name", "FIXME"
    )));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("highway", "primary")));
  }

  @Test
  public void testTagFilterNotEqualsAnyOf() {
    FilterExpression expression = parser.parse("highway in (residential, track)").negate();
    assertFalse(expression.applyOSM(createTestOSMEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("name", "FIXME")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode()));
    assertTrue(expression.applyOSM(createTestOSMEntityNode(
        "building", "yes",
        "highway", "primary",
        "name", "FIXME"
    )));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "primary")));
  }

  @Test
  public void testIdFilterEquals() {
    assertTrue(parser.parse("id:1").applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:2").applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testIdFilterNotEquals() {
    assertFalse(parser.parse("id:1").negate().applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:2").negate().applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testIdFilterEqualsAnyOf() {
    assertTrue(parser.parse("id:(1,2,3)").applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:(2,3)").applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testIdFilterNotEqualsAnyOf() {
    assertFalse(parser.parse("id:(1,2,3)").negate().applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:(2,3)").negate().applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testIdFilterInRange() {
    assertTrue(parser.parse("id:(1..3)").applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:(2..3)").applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:(1..)").applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:(2..)").applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:(..3)").applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:(..0)").applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testIdFilterNotInRange() {
    assertFalse(parser.parse("id:(1..3)").negate().applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:(2..3)").negate().applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:(1..)").negate().applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:(2..)").negate().applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("id:(..3)").negate().applyOSM(createTestOSMEntityNode()));
    assertTrue(parser.parse("id:(..0)").negate().applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testTypeFilter() {
    assertTrue(parser.parse("type:node").applyOSM(createTestOSMEntityNode()));
    assertFalse(parser.parse("type:way").applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testAndOperator() {
    FilterExpression expression = parser.parse("highway=residential and name=*");
    assertTrue(expression.applyOSM(createTestOSMEntityNode(
        "highway", "residential",
        "name", "FIXME"
    )));
    assertFalse(expression.applyOSM(createTestOSMEntityNode(
        "highway", "residential"
    )));
    assertFalse(expression.applyOSM(createTestOSMEntityNode(
        "name", "FIXME"
    )));
    assertFalse(expression.applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testOrOperator() {
    FilterExpression expression = parser.parse("highway=residential or name=*");
    assertTrue(expression.applyOSM(createTestOSMEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestOSMEntityNode("name", "FIXME")));
    assertFalse(expression.applyOSM(createTestOSMEntityNode("building", "yes")));
  }

  @Test
  public void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSM(createTestOSMEntityNode()));
    assertFalse(expression.applyOSM(createTestOSMEntityWay(new long[] {})));
    assertFalse(expression.applyOSM(createTestOSMEntityRelation()));
  }

  @Test
  public void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    assertTrue(expression.applyOSM(createTestOSMEntityWay(new long[] {})));
    assertTrue(expression.applyOSM(createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1})));
    assertFalse(expression.applyOSM(createTestOSMEntityNode()));
    assertFalse(expression.applyOSM(createTestOSMEntityRelation()));
  }

  @Test
  public void testGeometryTypeFilterPolygon() {
    FilterExpression expression = parser.parse("geometry:polygon");
    assertTrue(expression.applyOSM(createTestOSMEntityWay(new long[] {1, 2, 3, 1})));
    assertFalse(expression.applyOSM(createTestOSMEntityWay(new long[] {1, 2, 3, 4})));
    assertFalse(expression.applyOSM(createTestOSMEntityWay(new long[] {1, 2, 1})));
    assertTrue(expression.applyOSM(createTestOSMEntityRelation("type", "multipolygon")));
    assertTrue(expression.applyOSM(createTestOSMEntityRelation("type", "boundary")));
    assertFalse(expression.applyOSM(createTestOSMEntityRelation()));
    assertFalse(expression.applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testGeometryTypeFilterOther() {
    FilterExpression expression = parser.parse("geometry:other");
    assertFalse(expression.applyOSM(createTestOSMEntityNode()));
    assertTrue(expression.applyOSM(createTestOSMEntityWay(new long[] {})));
    assertTrue(expression.applyOSM(createTestOSMEntityRelation()));
  }

  @Test
  public void testConstant() {
    FilterExpression expression = parser.parse("");
    assertTrue(expression.applyOSM(createTestOSMEntityNode()));
  }

  @Test
  public void testGeometryFilterArea() throws IOException {
    FilterExpression expression = parser.parse("area:(1..2)");
    assertTrue(expression.applyOSM(createTestOSMEntityWay(new long[] {})));
  }

  @Test
  public void testGeometryFilterLength() throws IOException {
    FilterExpression expression = parser.parse("length:(1..2)");
    assertTrue(expression.applyOSM(createTestOSMEntityWay(new long[] {})));
  }
}
