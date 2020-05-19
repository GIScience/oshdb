package org.heigit.ohsome.filter;

import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for negation of filters.
 */
public class NegateTest {
  private TagTranslator tagTranslator;

  @Before
  public void setup() throws SQLException, ClassNotFoundException, OSHDBKeytablesNotFoundException {
    Class.forName("org.h2.Driver");
    this.tagTranslator = new TagTranslator(DriverManager.getConnection(
        "jdbc:h2:./src/test/resources/keytables;ACCESS_MODE_DATA=r",
        "sa", ""
    ));
  }

  @After
  public void teardown() throws SQLException {
    this.tagTranslator.getConnection().close();
  }

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

  private void testAllTypes(FilterExpression expression, FilterExpression negation) {
    OSMEntity node = new OSMNode(0, 1, null, 0, 0, new int[] {}, 0, 0);
    assertTrue(expression.applyOSM(node) != negation.applyOSM(node));
    OSMEntity way = new OSMWay(0, 1, null, 0, 0, new int[] {}, new OSMMember[] {});
    assertTrue(expression.applyOSM(way) != negation.applyOSM(way));
    OSMEntity relation = new OSMRelation(0, 1, null, 0, 0, new int[] {}, new OSMMember[] {});
    assertTrue(expression.applyOSM(relation) != negation.applyOSM(relation));
  }

  @Test
  public void testTypeFilter() {
    FilterExpression expression = new TypeFilter(OSMType.NODE);
    FilterExpression negation = expression.negate();
    testAllTypes(expression, negation);
  }

  @Test
  public void testAndOperator() {
    FilterExpression sub1 = new TypeFilter(OSMType.NODE);
    FilterExpression sub2 = new TypeFilter(OSMType.WAY);
    FilterExpression expression = BinaryOperator.fromOperator(sub1, BinaryOperator.Type.AND, sub2);
    assertTrue(expression instanceof AndOperator);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof OrOperator);
    testAllTypes(sub1, ((BinaryOperator) negation).getLeftOperand());
    testAllTypes(sub2, ((BinaryOperator) negation).getRightOperand());
  }

  @Test
  public void testOrOperator() {
    FilterExpression sub1 = new TypeFilter(OSMType.NODE);
    FilterExpression sub2 = new TypeFilter(OSMType.WAY);
    FilterExpression expression = BinaryOperator.fromOperator(sub1, BinaryOperator.Type.OR, sub2);
    assertTrue(expression instanceof OrOperator);
    FilterExpression negation = expression.negate();
    assertTrue(negation instanceof AndOperator);
    testAllTypes(sub1, ((BinaryOperator) negation).getLeftOperand());
    testAllTypes(sub2, ((BinaryOperator) negation).getRightOperand());
  }
}
