package org.heigit.ohsome.oshdb.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.junit.jupiter.api.Test;

/**
 * Tests the application of filters to OSH entities.
 */
class ApplyOSHTest extends FilterTest {

  @Test
  void testTagFilterEquals() throws IOException {
    FilterExpression expression = parser.parse("highway=residential");
    // matching tag (exact match)
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential"))
    ));
    // matching tag (partial match)
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "track"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "track"),
        createTestOSMEntityNode("building", "yes"))
    ));
    // no match
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes"))
    ));
  }

  @Test
  void testTagFilterEqualsAny() throws IOException {
    FilterExpression expression = parser.parse("highway=*");
    // matching tag
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "track"),
        createTestOSMEntityNode("building", "yes"))
    ));
    // no match
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes"))
    ));
  }

  @Test
  void testTagFilterNotEquals() throws IOException {
    FilterExpression expression = parser.parse("highway!=residential");
    // matching tag
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential"),
        createTestOSMEntityNode("building", "yes"))
    ));
    // no match
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes"))
    ));
  }

  @Test
  void testTagFilterNotEqualsAny() throws IOException {
    FilterExpression expression = parser.parse("highway!=*");
    // matching tag
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential"),
        createTestOSMEntityNode("building", "yes"))
    ));
    // no match
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes"))
    ));
  }

  @Test
  void testTagFilterEqualsAnyOf() throws IOException {
    FilterExpression expression = parser.parse("highway in (residential, track)");
    // matching tag
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential")
    )));
    // matching tag key only
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "primary")
    )));
    // no matching key
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes")
    )));
    // one exact matching in versions
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes"),
        createTestOSMEntityNode("highway", "track")
    )));
    // one partial matching in versions: should return true, even though no version actually matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("building", "yes"),
        createTestOSMEntityNode("highway", "primary")
    )));
  }

  @Test
  void testIdFilterEquals() throws IOException {
    assertTrue(parser.parse("id:1").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:2").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  void testIdFilterNotEquals() throws IOException {
    assertFalse(parser.parse("id:1").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:2").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  void testIdFilterEqualsAnyOf() throws IOException {
    assertTrue(parser.parse("id:(1,2,3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(2,3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  void testIdFilterNotEqualsAnyOf() throws IOException {
    assertFalse(parser.parse("id:(1,2,3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(2,3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  void testIdFilterInRange() throws IOException {
    assertTrue(parser.parse("id:(1..3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(2..3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(1..)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(2..)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(..3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(..0)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  void testIdFilterNotInRange() throws IOException {
    assertFalse(parser.parse("id:(1..3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(2..3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(1..)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(2..)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(..3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(..0)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  void testTypeFilter() throws IOException {
    assertTrue(parser.parse("type:node").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
    assertFalse(parser.parse("type:way").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
  }

  @Test
  void testAndOperator() throws IOException {
    FilterExpression expression = parser.parse("highway=* and name=*");
    // exact match
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(
            "highway", "residential",
            "name", "FIXME"
        )
    )));
    // one version matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(
            "highway", "residential"
        ),
        createTestOSMEntityNode(
            "highway", "residential",
            "name", "FIXME"
        )
    )));
    // no match, but OSH contains both tag keys because different versions have each one of them
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(
            "highway", "residential"
        ),
        createTestOSMEntityNode(
            "name", "FIXME"
        )
    )));
    // no match
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(
            "highway", "residential"
        )
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(
            "name", ""
        )
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
  }

  @Test
  void testOrOperator() throws IOException {
    FilterExpression expression = parser.parse("highway=* or name=*");
    // exact match
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode("highway", "residential")
    )));
    // one version matches
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(),
        createTestOSMEntityNode(
            "name", "FIXME"
        )
    )));
    // no match
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
  }

  @Test
  void testGeometryTypeFilterPoint() throws IOException {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityWay(
        createTestOSMEntityWay(new long[] {})
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityRelation(
        createTestOSMEntityRelation()
    )));
  }

  @Test
  void testGeometryTypeFilterLine() throws IOException {
    FilterExpression expression = parser.parse("geometry:line");
    assertTrue(expression.applyOSH(createTestOSHEntityWay(
        createTestOSMEntityWay(new long[] {})
    )));
    assertTrue(expression.applyOSH(createTestOSHEntityWay(
        createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1})
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityRelation(
        createTestOSMEntityRelation()
    )));
  }

  @Test
  void testGeometryTypeFilterPolygon() throws IOException {
    FilterExpression expression = parser.parse("geometry:polygon");
    assertTrue(expression.applyOSH(createTestOSHEntityWay(
        createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1})
    )));
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(
        createTestOSMEntityRelation("type", "multipolygon")
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
  }

  @Test
  void testGeometryTypeFilterOther() throws IOException {
    FilterExpression expression = parser.parse("geometry:other");
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
    assertTrue(expression.applyOSH(createTestOSHEntityWay(
        createTestOSMEntityWay(new long[] {})
    )));
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(
        createTestOSMEntityRelation()
    )));
  }

  @Test
  void testConstant() throws IOException {
    FilterExpression expression = parser.parse("");
    assertTrue(expression.applyOSH(createTestOSHEntityNode(createTestOSMEntityNode())));
  }

  @Test
  void testGeometryFilterArea() throws IOException {
    FilterExpression expression = parser.parse("area:(1..2)");
    assertTrue(expression.applyOSH(createTestOSHEntityWay(createTestOSMEntityWay(new long[] {}))));
  }

  @Test
  void testGeometryFilterLength() throws IOException {
    FilterExpression expression = parser.parse("length:(1..2)");
    assertTrue(expression.applyOSH(createTestOSHEntityWay(createTestOSMEntityWay(new long[] {}))));
  }

  private void testOSHEntityWithMetadata(FilterExpression expression) throws IOException {
    // a node
    assertTrue(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(1, 1),
        createTestOSMEntityNode(42, 4),
        createTestOSMEntityNode(100, 10)
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode(1, 1),
        createTestOSMEntityNode(100, 10)
    )));
    // a way
    assertTrue(expression.applyOSH(createTestOSHEntityWay(new OSMWay[] {
        createTestOSMEntityWay(42, 4, new long[] {})
    }, new OSHNode[] {})));
    assertTrue(expression.applyOSH(createTestOSHEntityWay(new OSMWay[] {
        createTestOSMEntityWay(1, 1, new long[] {})
    }, new OSHNode[] { createTestOSHEntityNode(
        createTestOSMEntityNode(42, 4)
    )})));
    // a relation
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(new OSMRelation[] {
        createTestOSMEntityRelation(42, 4)
    }, new OSHNode[] {}, new OSHWay[] {})));
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(new OSMRelation[] {
        createTestOSMEntityRelation(1, 1)
    }, new OSHNode[] { createTestOSHEntityNode(
        createTestOSMEntityNode(42, 4)
    )}, new OSHWay[] {})));
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(new OSMRelation[] {
        createTestOSMEntityRelation(1, 1)
    }, new OSHNode[] {}, new OSHWay[] { createTestOSHEntityWay(
        createTestOSMEntityWay(42, 4, new long[] {})
    )})));
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(new OSMRelation[] {
        createTestOSMEntityRelation(1, 1)
    }, new OSHNode[] {}, new OSHWay[] { createTestOSHEntityWay(new OSMWay[]{
        createTestOSMEntityWay(1, 1, new long[]{1})
    }, new OSHNode[] {createTestOSHEntityNode(
        createTestOSMEntityNode(42, 4)
    )})})));
  }

  @Test
  void testChangesetId() throws IOException {
    testOSHEntityWithMetadata(parser.parse("changeset:42"));
  }

  @Test
  void testChangesetIdList() throws IOException {
    testOSHEntityWithMetadata(parser.parse("changeset:(41,42,43)"));
  }

  @Test
  void testChangesetIdRange() throws IOException {
    testOSHEntityWithMetadata(parser.parse("changeset:(41..43)"));
  }

  @Test
  void testContributorUserId() throws IOException {
    var parser = new FilterParser(tagTranslator, true);
    testOSHEntityWithMetadata(parser.parse("contributor:4"));
  }

  @Test
  void testContributorUserIdList() throws IOException {
    var parser = new FilterParser(tagTranslator, true);
    testOSHEntityWithMetadata(parser.parse("contributor:(3,4,5)"));
  }

  @Test
  void testContributorUserIdRange() throws IOException {
    var parser = new FilterParser(tagTranslator, true);
    testOSHEntityWithMetadata(parser.parse("contributor:(3..5)"));
  }
}
