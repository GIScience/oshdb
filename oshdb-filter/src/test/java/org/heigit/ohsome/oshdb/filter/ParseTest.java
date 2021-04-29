package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.filter.GeometryTypeFilter.GeometryType;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.junit.Test;

/**
 * Tests the parsing of filters and the application to OSM entities.
 */
public class ParseTest extends FilterTest {
  @Test
  public void testTagFilterEquals() {
    FilterExpression expression = parser.parse("highway=residential");
    assertTrue(expression instanceof TagFilterEquals);
    OSHDBTag tag = tagTranslator.getOSHDBTagOf("highway", "residential");
    assertEquals(tag, ((TagFilterEquals) expression).getTag());
    assertEquals("tag:" + tag.getKey() + "=" + tag.getValue(), expression.toString());
  }

  @Test
  public void testTagFilterStrings() {
    // tag key with colon; quoted string as value
    assertTrue(parser.parse("addr:street=\"Hauptstraße\"") instanceof TagFilter);
    // whitespace (in string; between key and value, single quotes
    assertTrue(parser.parse("name = \"Colorado River\"") instanceof TagFilter);

    // "Allowed characters are: the letters `a-z` and `A-Z`, digits, underscore, dashes and colons."
    assertTrue(parser.parse("name=a0_-:") instanceof TagFilter);
  }

  @Test
  public void testTagFilterEqualsAny() {
    FilterExpression expression = parser.parse("highway=*");
    assertTrue(expression instanceof TagFilterEqualsAny);
    OSHDBTagKey tag = tagTranslator.getOSHDBTagKeyOf("highway");
    assertEquals(tag, ((TagFilterEqualsAny) expression).getTag());
    assertEquals("tag:" + tag.toInt() + "=*", expression.toString());
  }

  @Test
  public void testTagFilterNotEquals() {
    FilterExpression expression = parser.parse("highway!=residential");
    assertTrue(expression instanceof TagFilterNotEquals);
    OSHDBTag tag = tagTranslator.getOSHDBTagOf("highway", "residential");
    assertEquals(tag, ((TagFilterNotEquals) expression).getTag());
    assertEquals("tag:" + tag.getKey() + "!=" + tag.getValue(), expression.toString());
  }

  @Test
  public void testTagFilterNotEqualsAny() {
    FilterExpression expression = parser.parse("highway!=*");
    assertTrue(expression instanceof TagFilterNotEqualsAny);
    OSHDBTagKey tag = tagTranslator.getOSHDBTagKeyOf("highway");
    assertEquals(tag, ((TagFilterNotEqualsAny) expression).getTag());
    assertEquals("tag:" + tag.toInt() + "!=*", expression.toString());
  }

  @SuppressWarnings("RegExpDuplicateAlternationBranch") // false positive by intellij
  @Test
  public void testTagFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("highway in (residential, track)");
    assertTrue(expression instanceof TagFilterEqualsAnyOf);
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    assertTrue(expression.toString().matches("tag:" + tag1.getKey() + "in("
        + tag1.getValue() + "," + tag2.getValue() + "|"
        + tag2.getValue() + "," + tag1.getValue() + ")"
    ));
  }

  @SuppressWarnings("RegExpDuplicateAlternationBranch") // false positive by intellij
  @Test
  public void testTagFilterNotEqualsAnyOf() {
    FilterExpression expression = parser.parse("not highway in (residential, track)");
    assertTrue(expression instanceof TagFilterNotEqualsAnyOf);
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    assertTrue(expression.toString().matches("tag:" + tag1.getKey() + "not-in("
        + tag1.getValue() + "," + tag2.getValue() + "|"
        + tag2.getValue() + "," + tag1.getValue() + ")"
    ));
  }

  @Test(expected = IllegalStateException.class)
  public void testTagFilterEqualsAnyOfCheckEmpty() {
    new TagFilterEqualsAnyOf(Collections.emptyList());
  }

  @Test(expected = IllegalStateException.class)
  public void testTagFilterNotEqualsAnyOfCheckEmpty() {
    new TagFilterNotEqualsAnyOf(Collections.emptyList());
  }

  @Test(expected = IllegalStateException.class)
  public void testTagFilterEqualsAnyOfCheckMixed() {
    new TagFilterEqualsAnyOf(Arrays.asList(
        tagTranslator.getOSHDBTagOf("highway", "residential"),
        tagTranslator.getOSHDBTagOf("building", "yes")
    ));
  }

  @Test(expected = IllegalStateException.class)
  public void testTagFilterNotEqualsAnyOfCheckMixed() {
    new TagFilterNotEqualsAnyOf(Arrays.asList(
        tagTranslator.getOSHDBTagOf("highway", "residential"),
        tagTranslator.getOSHDBTagOf("building", "yes")
    ));
  }

  @Test
  public void testIdFilterEquals() {
    FilterExpression expression = parser.parse("id:123");
    assertTrue(expression instanceof IdFilterEquals);
    assertEquals(123, ((IdFilterEquals) expression).getId());
    assertEquals("id:123", expression.toString());
  }

  @Test
  public void testIdTypeFilterEquals() {
    FilterExpression expression = parser.parse("id:node/123");
    assertTrue(expression instanceof AndOperator);
    assertTrue(((AndOperator) expression).op1 instanceof TypeFilter
        || ((AndOperator) expression).op2 instanceof TypeFilter);
    assertTrue(((AndOperator) expression).op1 instanceof IdFilterEquals
        || ((AndOperator) expression).op2 instanceof IdFilterEquals);
  }

  @Test
  public void testIdFilterNotEquals() {
    FilterExpression expression = parser.parse("not id:123");
    assertTrue(expression instanceof IdFilterNotEquals);
    assertEquals(123, ((IdFilterNotEquals) expression).getId());
    assertEquals("not-id:123", expression.toString());
  }

  @Test
  public void testIdFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("id:(1,2,3)");
    assertTrue(expression instanceof IdFilterEqualsAnyOf);
    assertEquals("id:in1,2,3", expression.toString());
  }

  @Test
  public void testIdTypeFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("id:(node/1,way/2)");
    assertTrue(expression instanceof OrOperator);
    assertTrue(((OrOperator) expression).op1 instanceof AndOperator);
    assertTrue(((OrOperator) expression).op2 instanceof AndOperator);
    assertEquals("type:node and id:1 or type:way and id:2", expression.toString());
  }

  @Test(expected = IllegalStateException.class)
  public void testIdFilterEqualsAnyOfCheckEmpty() {
    new IdFilterEqualsAnyOf(Collections.emptyList());
  }

  @Test
  public void testIdFilterInRange() {
    // complete range
    FilterExpression expression = parser.parse("id:(1..3)");
    assertTrue(expression instanceof IdFilterRange);
    assertEquals("id:in-range1..3", expression.toString());
    // no min value
    expression = parser.parse("id:(..3)");
    assertTrue(expression instanceof IdFilterRange);
    assertEquals("id:in-range..3", expression.toString());
    // no max value
    expression = parser.parse("id:(1..)");
    assertTrue(expression instanceof IdFilterRange);
    assertEquals("id:in-range1..", expression.toString());
    // reverse order
    expression = parser.parse("id:(3..1)");
    assertTrue(expression instanceof IdFilterRange);
    assertEquals("id:in-range1..3", expression.toString());
  }

  @Test
  public void testTypeFilter() {
    FilterExpression expression = parser.parse("type:node");
    assertTrue(expression instanceof TypeFilter);
    assertEquals(OSMType.NODE, ((TypeFilter) expression).getType());
    assertEquals("type:node", expression.toString());
    assertEquals(OSMType.WAY, ((TypeFilter) parser.parse("type:way")).getType());
    assertEquals(OSMType.RELATION, ((TypeFilter) parser.parse("type:relation")).getType());
  }

  @Test
  public void testAndOperator() {
    FilterExpression expression = parser.parse("highway=residential and name=*");
    assertTrue(expression instanceof AndOperator);
    assertTrue(((AndOperator) expression).getLeftOperand() instanceof TagFilter);
    assertTrue(((AndOperator) expression).getRightOperand() instanceof TagFilter);
    assertTrue(expression.toString().contains(" and "));
  }

  @Test
  public void testOrOperator() {
    FilterExpression expression = parser.parse("highway=residential or name=*");
    assertTrue(expression instanceof OrOperator);
    assertTrue(((OrOperator) expression).getLeftOperand() instanceof TagFilter);
    assertTrue(((OrOperator) expression).getRightOperand() instanceof TagFilter);
    assertTrue(expression.toString().contains(" or "));
  }

  @Test
  public void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression instanceof GeometryTypeFilter);
    assertEquals(GeometryType.POINT, ((GeometryTypeFilter) expression).getGeometryType());
    assertEquals(
        Collections.singleton(OSMType.NODE),
        ((GeometryTypeFilter) expression).getOSMTypes()
    );
    assertEquals("geometry:point", expression.toString());
  }

  @Test
  public void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    assertTrue(expression instanceof GeometryTypeFilter);
    assertEquals(GeometryType.LINE, ((GeometryTypeFilter) expression).getGeometryType());
    assertEquals(
        Collections.singleton(OSMType.WAY),
        ((GeometryTypeFilter) expression).getOSMTypes()
    );
    assertEquals("geometry:line", expression.toString());
  }

  @Test
  public void testGeometryTypeFilterPolygon() {
    FilterExpression expression = parser.parse("geometry:polygon");
    assertTrue(expression instanceof GeometryTypeFilter);
    assertEquals(GeometryType.POLYGON, ((GeometryTypeFilter) expression).getGeometryType());
    assertEquals(
        EnumSet.of(OSMType.WAY, OSMType.RELATION),
        ((GeometryTypeFilter) expression).getOSMTypes()
    );
    assertEquals("geometry:polygon", expression.toString());
  }

  @Test
  public void testGeometryTypeFilterOther() {
    FilterExpression expression = parser.parse("geometry:other");
    assertTrue(expression instanceof GeometryTypeFilter);
    assertEquals(GeometryType.OTHER, ((GeometryTypeFilter) expression).getGeometryType());
    assertEquals(
        EnumSet.of(OSMType.WAY, OSMType.RELATION),
        ((GeometryTypeFilter) expression).getOSMTypes()
    );
    assertEquals("geometry:other", expression.toString());
  }

  @Test
  public void testPaddingWhitespace() {
    // allow extra whitespace at start/end of filter
    FilterExpression expression = parser.parse(" type:node ");
    assertTrue(expression instanceof TypeFilter);
  }

  @Test
  public void testParentheses() {
    FilterExpression expression =
        parser.parse("type:way and (highway=residential or highway=track)");
    assertTrue(expression instanceof AndOperator);
    assertTrue(((AndOperator) expression).getLeftOperand() instanceof TypeFilter);
    assertTrue(((AndOperator) expression).getRightOperand() instanceof OrOperator);
    String expressionString = expression.toString();
    // extra whitespace within parens
    assertEquals(expressionString,
        parser.parse("type:way and ( highway=residential or highway=track )").toString()
    );
    // omitted whitespace outside of parens
    assertEquals(expressionString,
        parser.parse("(type:way)and(highway=residential or highway=track)").toString()
    );
  }

  @Test
  public void testEmptyFilter() {
    forEmptyFilter("");
    forEmptyFilter(" ");
  }

  private void forEmptyFilter(String emptyFilter) {
    FilterExpression expression = parser.parse(emptyFilter);
    assertTrue(expression instanceof ConstantFilter);
    assertTrue(((ConstantFilter) expression).getState());
    assertEquals("true", expression.toString());
  }

  @Test
  public void testGeometryFilterArea() {
    FilterExpression expression = parser.parse("area:(1..10)");
    assertTrue(expression instanceof GeometryFilterArea);
    assertEquals(1.0, ((GeometryFilterArea) expression).getRange().getFromValue(), 1E-10);
    assertEquals(10.0, ((GeometryFilterArea) expression).getRange().getToValue(), 1E-10);
    assertEquals("area:1.0..10.0", expression.toString());
    expression = parser.parse("area:(1.1..10.0)");
    assertTrue(expression instanceof GeometryFilterArea);
    expression = parser.parse("area:(1.E-6..10.0)");
    assertTrue(expression instanceof GeometryFilterArea);
    expression = parser.parse("area:(1..)");
    assertTrue(expression instanceof GeometryFilterArea);
    assertEquals(1.0, ((GeometryFilterArea) expression).getRange().getFromValue(), 1E-10);
    assertEquals(Double.POSITIVE_INFINITY,
        ((GeometryFilterArea) expression).getRange().getToValue(), 1E-10);
    expression = parser.parse("area:(..1)");
    assertTrue(expression instanceof GeometryFilterArea);
    assertEquals(Double.NEGATIVE_INFINITY,
        ((GeometryFilterArea) expression).getRange().getFromValue(), 1E-10);
    assertEquals(1.0, ((GeometryFilterArea) expression).getRange().getToValue(), 1E-10);
  }

  @Test
  public void testGeometryFilterLength() {
    FilterExpression expression = parser.parse("length:(1..10)");
    assertTrue(expression instanceof GeometryFilterLength);
  }
}
