package org.heigit.bigspatialdata.oshdb.util.geometry.osmhistorytestdata;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygonal;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;


public class OSHDBGeometryBuilderTestOsmHistoryTestDataRelation {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestOsmHistoryTestDataRelation() {
    testData.add("./src/test/resources/different-timestamps/polygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }

  @Test
  public void test1() throws ParseException {
    // relation getting more ways, one disappears, last version not valid
    OSMEntity entity = testData.relations().get(500L).get(0);
    OSHDBTimestamp timestamp = entity.getTimestamp();
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertTrue(result instanceof MultiPolygon);
    assertTrue(result.isValid());
    assertEquals(9, result.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)))"
    );
    Geometry intersection = result.intersection(expectedPolygon);
    assertEquals(expectedPolygon.getArea(), intersection.getArea(), DELTA);
    // second version
    OSMEntity entity1 = testData.relations().get(500L).get(1);
    OSHDBTimestamp timestamp1 = entity1.getTimestamp();
    Geometry result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp1, areaDecider);
    assertTrue(result1 instanceof MultiPolygon);
    assertTrue(result1.isValid());
    assertEquals(14, result1.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon1 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "((7.34 1.05, 7.32 1.05, 7.32 1.04, 7.33 1.04, 7.34 1.05)),"
            + "(( 7.32 1.05,7.32 1.07,7.31 1.07,7.31 1.05,7.32 1.05)))"
    );
    Geometry intersection1 = result1.intersection(expectedPolygon1);
    assertEquals(expectedPolygon1.getArea(), intersection1.getArea(), DELTA);
    // third version
    OSMEntity entity2 = testData.relations().get(500L).get(2);
    OSHDBTimestamp timestamp2 = entity2.getTimestamp();
    try {
      Geometry result2 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp2, areaDecider);
      assertTrue(result2 instanceof GeometryCollection || result2 instanceof Polygonal);
      assertTrue(result2.getNumGeometries() == 3);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    /*
    assertTrue(result2 instanceof MultiPolygon);
    assertTrue(result2.isValid());
    assertEquals(14, result2.getCoordinates().length);

    // compare if coordinates of created points equals the coordinates of polygon
    Geometry expectedPolygon2 = (new WKTReader()).read(
        "MULTIPOLYGON(((7.31 1.01,7.34 1.01,7.34 1.05, 7.31 1.01)),"
            + "(( 7.32 1.05,7.32 1.07,7.31 1.07,7.31 1.05,7.32 1.05)),"
            + "((7.33 1.05,7.33 1.06,7.32 1.06,7.32 1.05,7.33 1.05)))"
    );
    Geometry intersection2 = result2.intersection(expectedPolygon2);
    assertEquals(expectedPolygon2.getArea(), intersection2.getArea(), DELTA);*/
  }
}
