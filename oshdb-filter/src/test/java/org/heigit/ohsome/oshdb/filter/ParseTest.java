package org.heigit.ohsome.oshdb.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.filter.GeometryTypeFilter.GeometryType;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.jparsec.error.ParserException;
import org.junit.jupiter.api.Test;

/**
 * Tests the parsing of filters and the application to OSM entities.
 */
class ParseTest extends FilterTest {
  @Test
  void testTagFilterEquals() {
    FilterExpression expression = parser.parse("highway=residential");
    assertTrue(expression instanceof TagFilterEquals);
    OSHDBTag tag = tagTranslator.getOSHDBTagOf("highway", "residential").get();
    assertEquals(tag, ((TagFilterEquals) expression).getTag());
    assertEquals("tag:" + tag.getKey() + "=" + tag.getValue(), expression.toString());
  }

  @Test
  void testTagFilterStrings() {
    // tag key with colon; quoted string as value
    assertTrue(parser.parse("addr:street=\"Hauptstraße\"") instanceof TagFilter);
    // whitespace (in string; between key and value, single quotes
    assertTrue(parser.parse("name = \"Colorado River\"") instanceof TagFilter);

    // "Allowed characters are: the letters `a-z` and `A-Z`, digits, underscore, dashes and colons."
    assertTrue(parser.parse("name=a0_-:") instanceof TagFilter);
  }

  @Test
  void testTagFilterEqualsAny() {
    FilterExpression expression = parser.parse("highway=*");
    assertTrue(expression instanceof TagFilterEqualsAny);
    OSHDBTagKey tag = tagTranslator.getOSHDBTagKeyOf("highway").get();
    assertEquals(tag, ((TagFilterEqualsAny) expression).getTag());
    assertEquals("tag:" + tag.toInt() + "=*", expression.toString());
  }

  @Test
  void testTagFilterNotEquals() {
    FilterExpression expression = parser.parse("highway!=residential");
    assertTrue(expression instanceof TagFilterNotEquals);
    OSHDBTag tag = tagTranslator.getOSHDBTagOf("highway", "residential").get();
    assertEquals(tag, ((TagFilterNotEquals) expression).getTag());
    assertEquals("tag:" + tag.getKey() + "!=" + tag.getValue(), expression.toString());
  }

  @Test
  void testTagFilterNotEqualsAny() {
    FilterExpression expression = parser.parse("highway!=*");
    assertTrue(expression instanceof TagFilterNotEqualsAny);
    OSHDBTagKey tag = tagTranslator.getOSHDBTagKeyOf("highway").get();
    assertEquals(tag, ((TagFilterNotEqualsAny) expression).getTag());
    assertEquals("tag:" + tag.toInt() + "!=*", expression.toString());
  }

  @SuppressWarnings("RegExpDuplicateAlternationBranch") // false positive by intellij
  @Test
  void testTagFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("highway in (residential, track)");
    assertTrue(expression instanceof TagFilterEqualsAnyOf);
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential").get();
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track").get();
    assertTrue(expression.toString().matches("tag:" + tag1.getKey() + "in("
        + tag1.getValue() + "," + tag2.getValue() + "|"
        + tag2.getValue() + "," + tag1.getValue() + ")"
    ));
  }

  @SuppressWarnings("RegExpDuplicateAlternationBranch") // false positive by intellij
  @Test
  void testTagFilterNotEqualsAnyOf() {
    FilterExpression expression = parser.parse("not highway in (residential, track)");
    assertTrue(expression instanceof TagFilterNotEqualsAnyOf);
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential").get();
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track").get();
    assertTrue(expression.toString().matches("tag:" + tag1.getKey() + "not-in("
        + tag1.getValue() + "," + tag2.getValue() + "|"
        + tag2.getValue() + "," + tag1.getValue() + ")"
    ));
  }

  @Test()
  void testTagFilterEqualsAnyOfCheckEmpty() {
    assertThrows(IllegalStateException.class, () -> {
      new TagFilterEqualsAnyOf(Collections.emptyList());
    });
  }

  @Test()
  void testTagFilterNotEqualsAnyOfCheckEmpty() {
    assertThrows(IllegalStateException.class, () -> {
      new TagFilterNotEqualsAnyOf(Collections.emptyList());
    });
  }

  @Test()
  void testTagFilterEqualsAnyOfCheckMixed() {
    assertThrows(IllegalStateException.class, () -> {
      new TagFilterEqualsAnyOf(Arrays.asList(
          tagTranslator.getOSHDBTagOf("highway", "residential").get(),
          tagTranslator.getOSHDBTagOf("building", "yes").get()
      ));
    });
  }

  @Test()
  void testTagFilterNotEqualsAnyOfCheckMixed() {
    assertThrows(IllegalStateException.class, () -> {
      new TagFilterNotEqualsAnyOf(Arrays.asList(
          tagTranslator.getOSHDBTagOf("highway", "residential").get(),
          tagTranslator.getOSHDBTagOf("building", "yes").get()
      ));
    });
  }

  @Test
  void testIdFilterEquals() {
    FilterExpression expression = parser.parse("id:123");
    assertTrue(expression instanceof IdFilterEquals);
    assertEquals(123, ((IdFilterEquals) expression).getId());
    assertEquals("id:123", expression.toString());
  }

  @Test
  void testIdTypeFilterEquals() {
    FilterExpression expression = parser.parse("id:node/123");
    assertTrue(expression instanceof AndOperator);
    assertTrue(((AndOperator) expression).op1 instanceof TypeFilter
        || ((AndOperator) expression).op2 instanceof TypeFilter);
    assertTrue(((AndOperator) expression).op1 instanceof IdFilterEquals
        || ((AndOperator) expression).op2 instanceof IdFilterEquals);
  }

  @Test
  void testIdFilterNotEquals() {
    FilterExpression expression = parser.parse("not id:123");
    assertTrue(expression instanceof IdFilterNotEquals);
    assertEquals(123, ((IdFilterNotEquals) expression).getId());
    assertEquals("not-id:123", expression.toString());
  }

  @Test
  void testIdFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("id:(1,2,3)");
    assertTrue(expression instanceof IdFilterEqualsAnyOf);
    assertEquals("id:in1,2,3", expression.toString());
    var filter = (IdFilterEqualsAnyOf) expression;
    assertTrue(filter.getIds().containsAll(List.of(1L, 2L, 3L)));
  }

  @Test
  void testIdTypeFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("id:(node/1,way/2)");
    assertTrue(expression instanceof OrOperator);
    assertTrue(((OrOperator) expression).op1 instanceof AndOperator);
    assertTrue(((OrOperator) expression).op2 instanceof AndOperator);
    assertEquals("type:node and id:1 or type:way and id:2", expression.toString());
  }

  @Test()
  void testIdFilterEqualsAnyOfCheckEmpty() {
    assertThrows(IllegalStateException.class, () -> {
      new IdFilterEqualsAnyOf(Collections.emptyList());
    });
  }

  @Test
  void testIdFilterInRange() {
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
  void testTypeFilter() {
    FilterExpression expression = parser.parse("type:node");
    assertTrue(expression instanceof TypeFilter);
    assertEquals(OSMType.NODE, ((TypeFilter) expression).getType());
    assertEquals("type:node", expression.toString());
    assertEquals(OSMType.WAY, ((TypeFilter) parser.parse("type:way")).getType());
    assertEquals(OSMType.RELATION, ((TypeFilter) parser.parse("type:relation")).getType());
  }

  @Test
  void testAndOperator() {
    FilterExpression expression = parser.parse("highway=residential and name=*");
    assertTrue(expression instanceof AndOperator);
    assertTrue(((AndOperator) expression).getLeftOperand() instanceof TagFilter);
    assertTrue(((AndOperator) expression).getRightOperand() instanceof TagFilter);
    assertTrue(expression.toString().contains(" and "));
  }

  @Test
  void testOrOperator() {
    FilterExpression expression = parser.parse("highway=residential or name=*");
    assertTrue(expression instanceof OrOperator);
    assertTrue(((OrOperator) expression).getLeftOperand() instanceof TagFilter);
    assertTrue(((OrOperator) expression).getRightOperand() instanceof TagFilter);
    assertTrue(expression.toString().contains(" or "));
  }

  @Test
  void testGeometryTypeFilterPoint() {
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
  void testGeometryTypeFilterLine() {
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
  void testGeometryTypeFilterPolygon() {
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
  void testGeometryTypeFilterOther() {
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
  void testPaddingWhitespace() {
    // allow extra whitespace at start/end of filter
    FilterExpression expression = parser.parse(" type:node ");
    assertTrue(expression instanceof TypeFilter);
  }

  @Test
  void testParentheses() {
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
  void testEmptyFilter() {
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
  void testGeometryFilterArea() {
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
  void testGeometryFilterLength() {
    FilterExpression expression = parser.parse("length:(1..10)");
    assertTrue(expression instanceof GeometryFilterLength);
  }

  @Test
  void testGeometryFilterPerimeter() {
    FilterExpression expression = parser.parse("perimeter:(1..10)");
    assertTrue(expression instanceof GeometryFilterPerimeter);
  }

  @Test
  void testGeometryFilterVertices() {
    FilterExpression expression = parser.parse("geometry.vertices:(1..10)");
    assertTrue(expression instanceof GeometryFilterVertices);
  }

  @Test
  void testGeometryFilterOuters() {
    FilterExpression expression = parser.parse("geometry.outers:2");
    assertTrue(expression instanceof GeometryFilterOuterRings);
    expression = parser.parse("geometry.outers:(1..10)");
    assertTrue(expression instanceof GeometryFilterOuterRings);
  }

  @Test
  void testGeometryFilterInners() {
    FilterExpression expression = parser.parse("geometry.inners:0");
    assertTrue(expression instanceof GeometryFilterInnerRings);
    expression = parser.parse("geometry.inners:(1..10)");
    assertTrue(expression instanceof GeometryFilterInnerRings);
  }

  @Test
  void testGeometryFilterRoundness() {
    FilterExpression expression = parser.parse("geometry.roundness:(0.8..)");
    assertTrue(expression instanceof GeometryFilterRoundness);
  }

  @Test
  void testGeometryFilterSquareness() {
    FilterExpression expression = parser.parse("geometry.squareness:(0.8..)");
    assertTrue(expression instanceof GeometryFilterSquareness);
  }

  @Test
  void testChangesetIdFilter() {
    FilterExpression expression = parser.parse("changeset:42");
    assertTrue(expression instanceof ChangesetIdFilterEquals);
    assertEquals(42, ((ChangesetIdFilterEquals) expression).getChangesetId());
    assertEquals("changeset:42", expression.toString());
  }

  @Test
  void testChangesetIdListFilter() {
    FilterExpression expression = parser.parse("changeset:(1,2,3)");
    assertTrue(expression instanceof ChangesetIdFilterEqualsAnyOf);
    assertEquals(List.of(1L, 2L, 3L), new ArrayList<>(
        ((ChangesetIdFilterEqualsAnyOf) expression).getChangesetIdList()));
    assertEquals("changeset:in(1,2,3)", expression.toString());
  }

  @Test
  void testChangesetIdRangeFilter() {
    FilterExpression expression = parser.parse("changeset:(10..12)");
    assertTrue(expression instanceof ChangesetIdFilterRange);
    assertEquals("changeset:in-range10..12", expression.toString());
  }

  @Test
  void testContributorIdFilterEnabled() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    FilterExpression expression = parser.parse("contributor:1" /* Steve <3 */);
    assertTrue(expression instanceof ContributorUserIdFilterEquals);
    assertEquals(1, ((ContributorUserIdFilterEquals) expression).getUserId());
    assertEquals("contributor:1", expression.toString());
  }

  @Test
  void testContributorUserIdListFilter() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    FilterExpression expression = parser.parse("contributor:(1,2,3)");
    assertTrue(expression instanceof ContributorUserIdFilterEqualsAnyOf);
    assertEquals(List.of(1, 2, 3),  new ArrayList<>(
        ((ContributorUserIdFilterEqualsAnyOf) expression).getContributorUserIdList()));
    assertEquals("contributor:in(1,2,3)", expression.toString());
  }

  @Test
  void testContributorUserIdRangeFilter() {
    FilterParser parser = new FilterParser(tagTranslator, true);
    FilterExpression expression = parser.parse("contributor:(10..12)");
    assertTrue(expression instanceof ContributorUserIdFilterRange);
    assertEquals("contributor:in-range10..12", expression.toString());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test()
  void testContributorIdFilterNotEnabled() {
    assertThrows(ParserException.class, () -> {
      parser.parse("contributor:0");
    });
  }
}
