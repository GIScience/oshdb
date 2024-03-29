package org.heigit.ohsome.oshdb.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests helper methods for creating filters from lambda functions.
 */
class FilterByTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();
  private final OSMNode testOSMEntity = createTestOSMEntityNode();
  private final OSHNode testOSHEntity = createTestOSHEntityNode(testOSMEntity);

  FilterByTest() throws IOException {
  }

  @Test
  void testFilterByOSHEntity() {
    FilterExpression expression;
    expression = Filter.byOSHEntity(e -> e.getId() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = expression.negate();
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  void testFilterByOSMEntity() {
    FilterExpression expression;
    expression = Filter.byOSMEntity(e -> e.getVersion() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = expression.negate();
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  void testFilterByOSMEntityApplyGeometryFallback() {
    FilterExpression expression = Filter.byOSMEntity(e -> e.getVersion() == 1);
    assertTrue(expression.applyOSMGeometry(testOSMEntity, gf.createPoint()));
    assertFalse(expression.negate().applyOSMGeometry(testOSMEntity, gf.createPoint()));
  }
}
