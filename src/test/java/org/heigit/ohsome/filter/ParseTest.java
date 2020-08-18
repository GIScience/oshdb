package org.heigit.ohsome.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.filter.GeometryTypeFilter.GeometryType;
import org.junit.Test;

/**
 * Test class for the ohsome-filter package.
 *
 * <p>Tests the parsing of filters and the application to OSM entities.</p>
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

  @Test
  public void testTagFilterEqualsAnyOf() {
    FilterExpression expression = parser.parse("highway in (residential, track)");
    assertTrue(expression instanceof TagFilterEqualsAnyOf);
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    //noinspection RegExpDuplicateAlternationBranch - false positive by intellij
    assertTrue(expression.toString().matches("tag:" + tag1.getKey() + "in("
        + tag1.getValue() + "," + tag2.getValue() + "|"
        + tag2.getValue() + "," + tag1.getValue() + ")"
    ));
  }

  @Test
  public void testTagFilterNotEqualsAnyOf() {
    FilterExpression expression = parser.parse("highway in (residential, track)").negate();
    assertTrue(expression instanceof TagFilterNotEqualsAnyOf);
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    //noinspection RegExpDuplicateAlternationBranch - false positive by intellij
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
        Collections.singleton(OSMType.RELATION),
        ((GeometryTypeFilter) expression).getOSMTypes()
    );
    assertEquals("geometry:other", expression.toString());
  }

  @Test
  public void testEmptyFilter() {
    FilterExpression expression = parser.parse("");
    assertTrue(expression instanceof ConstantFilter);
    assertTrue(((ConstantFilter) expression).getState());
    assertEquals("true", expression.toString());

    expression = parser.parse(" ");
    assertTrue(expression instanceof ConstantFilter);
    assertTrue(((ConstantFilter) expression).getState());
    assertEquals("true", expression.toString());
  }
}
