package org.heigit.bigspatialdata.oshdb.util.geometry.relations;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

public class OSHDBGeometryBuilderTestRelationTypeNotMultipolygonTest {
  private final OSMXmlReader testData = new OSMXmlReader();
  private final TagInterpreter tagInterpreter;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestRelationTypeNotMultipolygonTest() {
    testData.add("./src/test/resources/relations/relationTypeNotMultipolygon.osm");
    tagInterpreter = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void testTypeRestriction() {
    // relation type restriction
    OSMEntity entity1 = testData.relations().get(710900L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection );
      assertTrue(result.getNumGeometries() == 3);
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof Point);
      assertTrue(result.getGeometryN(2) instanceof LineString);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTypeAssociatedStreet() {
    // relation type associatedStreet
    OSMEntity entity1 = testData.relations().get(710901L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection );
      assertTrue(result.getNumGeometries() == 3);
      assertTrue(result.getGeometryN(0) instanceof Point);
      assertTrue(result.getGeometryN(1) instanceof Point);
      assertTrue(result.getGeometryN(2) instanceof Point);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTypePublicTransport() {
    // relation type public_transport
    OSMEntity entity1 = testData.relations().get(710902L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection );
      assertTrue(result.getNumGeometries() == 4);
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof Point);
      assertTrue(result.getGeometryN(2) instanceof LineString);
      assertTrue(result.getGeometryN(3) instanceof Point);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

  @Test
  public void testTypeBuilding() {
    // relation type building
    OSMEntity entity1 = testData.relations().get(710903L).get(0);
    try {
      Geometry result = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, tagInterpreter);
      assertTrue(result instanceof GeometryCollection );
      assertTrue(result.getNumGeometries() == 3);
      assertTrue(result.getGeometryN(0) instanceof LineString);
      assertTrue(result.getGeometryN(1) instanceof LineString);
      assertTrue(result.getGeometryN(2) instanceof LineString);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
  }

}

