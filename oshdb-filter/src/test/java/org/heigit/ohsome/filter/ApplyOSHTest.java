package org.heigit.ohsome.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.junit.Test;

/**
 * Test class for the ohsome-filter package.
 *
 * <p>Tests the parsing of filters and the application to OSM entities.</p>
 */
public class ApplyOSHTest extends FilterTest {

  @Test
  public void testTagFilterEquals() throws IOException {
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
  public void testTagFilterEqualsAny() throws IOException {
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
  public void testTagFilterNotEquals() throws IOException {
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
  public void testTagFilterNotEqualsAny() throws IOException {
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
  public void testTagFilterEqualsAnyOf() throws IOException {
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
  public void testIdFilterEquals() throws IOException {
    assertTrue(parser.parse("id:1").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:2").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  public void testIdFilterNotEquals() throws IOException {
    assertFalse(parser.parse("id:1").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:2").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  public void testIdFilterEqualsAnyOf() throws IOException {
    assertTrue(parser.parse("id:(1,2,3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertFalse(parser.parse("id:(2,3)").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  public void testIdFilterNotEqualsAnyOf() throws IOException {
    assertFalse(parser.parse("id:(1,2,3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
    assertTrue(parser.parse("id:(2,3)").negate().applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode())));
  }

  @Test
  public void testIdFilterInRange() throws IOException {
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
  public void testIdFilterNotInRange() throws IOException {
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
  public void testTypeFilter() throws IOException {
    assertTrue(parser.parse("type:node").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
    assertFalse(parser.parse("type:way").applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
  }

  @Test
  public void testAndOperator() throws IOException {
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
  public void testOrOperator() throws IOException {
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
  public void testGeometryTypeFilterPoint() throws IOException {
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
  public void testGeometryTypeFilterLine() throws IOException {
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
  public void testGeometryTypeFilterPolygon() throws IOException {
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
  public void testGeometryTypeFilterOther() throws IOException {
    FilterExpression expression = parser.parse("geometry:other");
    assertFalse(expression.applyOSH(createTestOSHEntityWay(
        createTestOSMEntityWay(new long[] {})
    )));
    assertFalse(expression.applyOSH(createTestOSHEntityNode(
        createTestOSMEntityNode()
    )));
    assertTrue(expression.applyOSH(createTestOSHEntityRelation(
        createTestOSMEntityRelation()
    )));
  }

  @Test
  public void testConstant() throws IOException {
    FilterExpression expression = parser.parse("");
    assertTrue(expression.applyOSH(createTestOSHEntityNode(createTestOSMEntityNode())));
  }

  @Test
  public void testGeometryFilterArea() throws IOException {
    FilterExpression expression = parser.parse("area:(1..2)");
    assertTrue(expression.applyOSH(createTestOSHEntityWay(createTestOSMEntityWay(new long[] {}))));
  }

  @Test
  public void testGeometryFilterLength() throws IOException {
    FilterExpression expression = parser.parse("length:(1..2)");
    assertTrue(expression.applyOSH(createTestOSHEntityWay(createTestOSMEntityWay(new long[] {}))));
  }
}
