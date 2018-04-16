package org.heigit.bigspatialdata.oshdb.util.geometry.incomplete;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.TimestampParser;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.junit.Test;

public class OSHDBGeometryBuilderTestWayIncompleteData {
  private final OSMXmlReader testData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final OSHDBTimestamp timestamp =
      TimestampParser.toOSHDBTimestamp("2014-01-01T00:00:00Z");
  private final double DELTA = 1E-6;

  public OSHDBGeometryBuilderTestWayIncompleteData() {
    testData.add("./src/test/resources/incomplete-osm/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(testData);
  }


  @Test
  public void test1() {
    // Way with four node references, one node missing
    // TODO https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/140
    OSMEntity entity1 = testData.ways().get(100L).get(0);
    Geometry result1 = null;
    try {
      result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    assertTrue(result1 instanceof LineString);
    assertTrue(result1.isValid());
    assertTrue(result1.getCoordinates().length >= 3);
  }

  @Test
  public void test2() {
    // Way with four nodes, area = yes
    // TODO https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/issues/140
    OSMEntity entity1 = testData.ways().get(101L).get(0);
    Geometry result1 = null;
    try {
      result1 = OSHDBGeometryBuilder.getGeometry(entity1, timestamp, areaDecider);
    }
    catch(Exception e){
      e.printStackTrace();
      fail("Should not have thrown any exception");
    }
    assertTrue(result1 instanceof LineString);
    assertTrue(result1.isValid());
    assertTrue(result1.getCoordinates().length >= 3);
  }



}
