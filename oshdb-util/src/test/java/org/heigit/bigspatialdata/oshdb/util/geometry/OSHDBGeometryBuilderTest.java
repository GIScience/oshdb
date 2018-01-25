package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class OSHDBGeometryBuilderTest {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBGeometryBuilderTest.class);

  public OSHDBGeometryBuilderTest() {}

  /*todo: @Test
  public void testIsArea() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, new OSHDBTimestamp(0L), 1L, 1, properties, 1000000000L, 1000000000L);
    TagInterpreter areaDecider = null;
    boolean expResult = false;
    boolean result = areaDecider.isArea(entity);
    assertEquals(expResult, result);
  }*/

  /*todo: @Test
  public void testIsLine() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, new OSHDBTimestamp(0L), 1L, 1, properties, 1000000000L, 1000000000L);
    TagInterpreter areaDecider = null;
    boolean expResult = false;
    boolean result = areaDecider.isLine(entity);
    assertEquals(expResult, result);
  }*/

  @Test
  public void testGetGeometry() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, new OSHDBTimestamp(0L), 1L, 1, properties, 1000000000L, 1000000000L);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(0L);
    TagInterpreter areaDecider = null;
    Geometry expResult = (new GeometryFactory()).createPoint(new Coordinate(100, 100));
    Geometry result = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetGeometryClipped_4args_1() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, new OSHDBTimestamp(0L), 1L, 1, properties, 1000000000L, 800000000L);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(0L);
    TagInterpreter areaDecider = null;
    OSHDBBoundingBox clipBbox = new OSHDBBoundingBox(-180.0, -90.0, 180.0, 90.0);
    Geometry expResult = (new GeometryFactory()).createPoint(new Coordinate(100, 80));
    Geometry result =
        OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, areaDecider, clipBbox);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetGeometryClipped_4args_2() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, new OSHDBTimestamp(0L), 1L, 1, properties, 1000000000L, 800000000L);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(0L);
    TagInterpreter areaDecider = null;
    Polygon clipPoly = OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(-180, -90, 180, 90));
    Geometry expResult = (new GeometryFactory()).createPoint(new Coordinate(100, 80));
    Geometry result =
        OSHDBGeometryBuilder.getGeometryClipped(entity, timestamp, areaDecider, clipPoly);
    assertEquals(expResult, result);
  }

}
