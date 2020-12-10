package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.junit.Test;

/**
 * Tests for normalization of filters.
 */
public class NormalizeTest {
  @Test
  public void testAndOperator() {
    FilterExpression sub1 = new TypeFilter(OSMType.NODE);
    FilterExpression sub2 = new TypeFilter(OSMType.WAY);
    FilterExpression expression = BinaryOperator.fromOperator(sub1, BinaryOperator.Type.AND, sub2);
    List<List<Filter>> norm = expression.normalize();
    assertEquals(1, norm.size());
    assertEquals(2, norm.get(0).size());
  }

  @Test
  public void testOrOperator() {
    FilterExpression sub1 = new TypeFilter(OSMType.NODE);
    FilterExpression sub2 = new TypeFilter(OSMType.WAY);
    FilterExpression expression = BinaryOperator.fromOperator(sub1, BinaryOperator.Type.OR, sub2);
    List<List<Filter>> norm = expression.normalize();
    assertEquals(2, norm.size());
    assertEquals(1, norm.get(0).size());
    assertEquals(1, norm.get(1).size());
  }
}
