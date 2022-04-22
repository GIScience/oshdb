package org.heigit.ohsome.oshdb.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.filter.GeometryTypeFilter.GeometryType;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagKey;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests for negation of filters.
 */
class NegateTest extends FilterTest {
  @Test
  void testTagFilterEquals() {
    FilterExpression expression = TagFilter.fromSelector(
        TagFilter.Type.EQUALS,
        new OSMTag("highway", "residential"),
        tagTranslator
    );
    assertTrue(expression instanceof TagFilterEquals);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterNotEquals);
  }

  @Test
  void testTagFilterNotEquals() {
    FilterExpression expression = TagFilter.fromSelector(
        TagFilter.Type.NOT_EQUALS,
        new OSMTag("highway", "residential"),
        tagTranslator
    );
    assertTrue(expression instanceof TagFilterNotEquals);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterEquals);
  }

  @Test
  void testTagFilterEqualsAny() {
    FilterExpression expression = TagFilter.fromSelector(
        TagFilter.Type.EQUALS,
        new OSMTagKey("highway"),
        tagTranslator
    );
    assertTrue(expression instanceof TagFilterEqualsAny);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterNotEqualsAny);
  }

  @Test
  void testTagFilterNotEqualsAny() {
    FilterExpression expression = TagFilter.fromSelector(
        TagFilter.Type.NOT_EQUALS,
        new OSMTagKey("highway"),
        tagTranslator
    );
    assertTrue(expression instanceof TagFilterNotEqualsAny);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterEqualsAny);
  }

  @Test
  void testTagFilterEqualsAnyOf() {
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    FilterExpression expression = new TagFilterEqualsAnyOf(Arrays.asList(tag1, tag2));
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterNotEqualsAnyOf);
  }

  @Test
  void testTagFilterNotEqualsAnyOf() {
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    FilterExpression expression = new TagFilterNotEqualsAnyOf(Arrays.asList(tag1, tag2));
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterEqualsAnyOf);
  }

  @Test
  void testIdEqualsFilter() {
    FilterExpression expression = new IdFilterEquals(1);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof IdFilterNotEquals);
  }

  @Test
  void testIdNotEqualsFilter() {
    FilterExpression expression = new IdFilterNotEquals(1);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof IdFilterEquals);
  }

  @Test
  void testIdEqualsAnyOfFilter() {
    FilterExpression expression = new IdFilterEqualsAnyOf(Collections.singletonList(1L));
    FilterExpression negation = expression.negate();
    OSMEntity testEntity = createTestOSMEntityNode();
    assertNotEquals(expression.applyOSM(testEntity), negation.applyOSM(testEntity));
    FilterExpression doubleNegation = negation.negate();
    assertEquals(expression.applyOSM(testEntity), doubleNegation.applyOSM(testEntity));
  }

  @Test
  void testIdInRangeFilter() {
    FilterExpression expression = new IdFilterRange(new IdRange(1, 3));
    FilterExpression negation = expression.negate();
    OSMEntity testEntity = createTestOSMEntityNode();
    assertNotEquals(expression.applyOSM(testEntity), negation.applyOSM(testEntity));
    FilterExpression doubleNegation = negation.negate();
    assertEquals(expression.applyOSM(testEntity), doubleNegation.applyOSM(testEntity));
  }

  private void testAllOSMTypes(FilterExpression expression, FilterExpression negation) {
    OSMEntity node = createTestOSMEntityNode();
    assertTrue(expression.applyOSM(node) != negation.applyOSM(node));
    OSMEntity way = createTestOSMEntityWay(new long[] {});
    assertTrue(expression.applyOSM(way) != negation.applyOSM(way));
    OSMEntity relation = createTestOSMEntityRelation();
    assertTrue(expression.applyOSM(relation) != negation.applyOSM(relation));
  }

  @Test
  void testTypeFilter() {
    FilterExpression expression = new TypeFilter(OSMType.NODE);
    FilterExpression negation = expression.negate();
    testAllOSMTypes(expression, negation);
  }

  @Test
  void testAndOperator() {
    FilterExpression sub1 = new TypeFilter(OSMType.NODE);
    FilterExpression sub2 = new TypeFilter(OSMType.WAY);
    FilterExpression expression = BinaryOperator.fromOperator(sub1, BinaryOperator.Type.AND, sub2);
    assertTrue(expression instanceof AndOperator);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof OrOperator);
    testAllOSMTypes(sub1, ((BinaryOperator) negation).getLeftOperand());
    testAllOSMTypes(sub2, ((BinaryOperator) negation).getRightOperand());
  }

  @Test
  void testOrOperator() {
    FilterExpression sub1 = new TypeFilter(OSMType.NODE);
    FilterExpression sub2 = new TypeFilter(OSMType.WAY);
    FilterExpression expression = BinaryOperator.fromOperator(sub1, BinaryOperator.Type.OR, sub2);
    assertTrue(expression instanceof OrOperator);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof AndOperator);
    testAllOSMTypes(sub1, ((BinaryOperator) negation).getLeftOperand());
    testAllOSMTypes(sub2, ((BinaryOperator) negation).getRightOperand());
  }

  private void testAllGeometryTypes(FilterExpression expression, FilterExpression negation) {
    GeometryFactory gf = new GeometryFactory();
    OSMEntity node = createTestOSMEntityNode();
    assertTrue(expression.applyOSMGeometry(node, gf.createPoint())
        != negation.applyOSMGeometry(node, gf.createPoint()));
    OSMEntity way = createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(way, gf.createLineString())
        != negation.applyOSMGeometry(way, gf.createLineString()));
    assertTrue(expression.applyOSMGeometry(way, gf.createPolygon())
        != negation.applyOSMGeometry(way, gf.createPolygon()));
    OSMEntity relation = createTestOSMEntityRelation("type", "multipolygon");
    assertTrue(expression.applyOSMGeometry(relation, gf.createPolygon())
        != negation.applyOSMGeometry(relation, gf.createPolygon()));
    assertTrue(expression.applyOSMGeometry(relation, gf.createGeometryCollection())
        != negation.applyOSMGeometry(relation, gf.createGeometryCollection()));
  }

  @Test
  void testGeometryTypePoint() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.POINT, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  void testGeometryTypeLine() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.LINE, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  void testGeometryTypePolygon() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.POLYGON, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  void testGeometryTypeOther() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.OTHER, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  void testConstant() {
    ConstantFilter expression = new ConstantFilter(true);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof ConstantFilter);
    assertNotEquals(expression.getState(), ((ConstantFilter) negation).getState());
    OSMEntity node = createTestOSMEntityNode();
    assertNotEquals(expression.applyOSM(node), negation.applyOSM(node));
  }
}
