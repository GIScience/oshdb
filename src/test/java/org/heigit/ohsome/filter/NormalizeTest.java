package org.heigit.ohsome.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for negation of filters.
 */
public class NormalizeTest {
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
