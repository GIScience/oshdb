package org.heigit.ohsome.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.ohsome.filter.GeometryFilter.ValueRange;
import org.heigit.ohsome.filter.GeometryTypeFilter.GeometryType;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests for negation of filters.
 */
public class NegateTest extends FilterTest {
  @Test
  public void testTagFilterEquals() {
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
  public void testTagFilterNotEquals() {
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
  public void testTagFilterEqualsAny() {
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
  public void testTagFilterNotEqualsAny() {
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
  public void testTagFilterEqualsAnyOf() {
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    FilterExpression expression = new TagFilterEqualsAnyOf(Arrays.asList(tag1, tag2));
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterNotEqualsAnyOf);
  }

  @Test
  public void testTagFilterNotEqualsAnyOf() {
    OSHDBTag tag1 = tagTranslator.getOSHDBTagOf("highway", "residential");
    OSHDBTag tag2 = tagTranslator.getOSHDBTagOf("highway", "track");
    FilterExpression expression = new TagFilterNotEqualsAnyOf(Arrays.asList(tag1, tag2));
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof TagFilterEqualsAnyOf);
  }

  @Test
  public void testIdEqualsFilter() {
    FilterExpression expression = new IdFilterEquals(1);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof IdFilterNotEquals);
  }

  @Test
  public void testIdNotEqualsFilter() {
    FilterExpression expression = new IdFilterNotEquals(1);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof IdFilterEquals);
  }

  @Test
  public void testIdEqualsAnyOfFilter() {
    FilterExpression expression = new IdFilterEqualsAnyOf(Collections.singletonList(1L));
    FilterExpression negation = expression.negate();
    OSMEntity testEntity = createTestEntityNode();
    assertNotEquals(expression.applyOSM(testEntity), negation.applyOSM(testEntity));
    FilterExpression doubleNegation = negation.negate();
    assertEquals(expression.applyOSM(testEntity), doubleNegation.applyOSM(testEntity));
  }

  @Test
  public void testIdInRangeFilter() {
    FilterExpression expression = new IdFilterRange(new IdFilterRange.IdRange(1, 3));
    FilterExpression negation = expression.negate();
    OSMEntity testEntity = createTestEntityNode();
    assertNotEquals(expression.applyOSM(testEntity), negation.applyOSM(testEntity));
    FilterExpression doubleNegation = negation.negate();
    assertEquals(expression.applyOSM(testEntity), doubleNegation.applyOSM(testEntity));
  }

  private void testAllOSMTypes(FilterExpression expression, FilterExpression negation) {
    OSMEntity node = createTestEntityNode();
    assertTrue(expression.applyOSM(node) != negation.applyOSM(node));
    OSMEntity way = createTestEntityWay(new long[] {});
    assertTrue(expression.applyOSM(way) != negation.applyOSM(way));
    OSMEntity relation = createTestEntityRelation();
    assertTrue(expression.applyOSM(relation) != negation.applyOSM(relation));
  }

  @Test
  public void testTypeFilter() {
    FilterExpression expression = new TypeFilter(OSMType.NODE);
    FilterExpression negation = expression.negate();
    testAllOSMTypes(expression, negation);
  }

  @Test
  public void testAndOperator() {
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
  public void testOrOperator() {
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
    OSMEntity node = createTestEntityNode();
    assertTrue(expression.applyOSMGeometry(node, gf.createPoint())
        != negation.applyOSMGeometry(node, gf.createPoint()));
    OSMEntity way = createTestEntityWay(new long[] {1,2,3,4,1});
    assertTrue(expression.applyOSMGeometry(way, gf.createLineString())
        != negation.applyOSMGeometry(way, gf.createLineString()));
    assertTrue(expression.applyOSMGeometry(way, gf.createPolygon())
        != negation.applyOSMGeometry(way, gf.createPolygon()));
    OSMEntity relation = createTestEntityRelation("type", "multipolygon");
    assertTrue(expression.applyOSMGeometry(relation, gf.createPolygon())
        != negation.applyOSMGeometry(relation, gf.createPolygon()));
    assertTrue(expression.applyOSMGeometry(relation, gf.createGeometryCollection())
        != negation.applyOSMGeometry(relation, gf.createGeometryCollection()));
  }

  @Test
  public void testGeometryTypePoint() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.POINT, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  public void testGeometryTypeLine() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.LINE, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  public void testGeometryTypePolygon() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.POLYGON, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  public void testGeometryTypeOther() {
    GeometryTypeFilter expression = new GeometryTypeFilter(GeometryType.OTHER, tagTranslator);
    FilterExpression negation = expression.negate();
    testAllGeometryTypes(expression, negation);
  }

  @Test
  public void testConstant() {
    ConstantFilter expression = new ConstantFilter(true);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof ConstantFilter);
    assertNotEquals(expression.getState(), ((ConstantFilter) negation).getState());
    OSMEntity node = createTestEntityNode();
    assertNotEquals(expression.applyOSM(node), negation.applyOSM(node));
  }
}
