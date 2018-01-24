package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import static org.heigit.bigspatialdata.oshdb.util.export.JSONTransformerTest.LONLAT_A;
import static org.heigit.bigspatialdata.oshdb.util.export.JSONTransformerTest.TAGS_A;
import static org.heigit.bigspatialdata.oshdb.util.export.JSONTransformerTest.USER_A;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class OSHDbGeometryBuilderTest {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDbGeometryBuilderTest.class);

  public OSHDbGeometryBuilderTest() {}

  @Test
  public void testGetGeometry() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, 0L, 1L, 1, properties, 1000000000L, 1000000000L);
    long timestamp = 0L;
    TagInterpreter areaDecider = null;
    Geometry expResult = (new GeometryFactory()).createPoint(new Coordinate(100, 100));
    Geometry result = OSHDbGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetGeometryClipped_4args_1() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, 0L, 1L, 1, properties, 1000000000L, 800000000L);
    long timestamp = 0L;
    TagInterpreter areaDecider = null;
    BoundingBox clipBbox = new BoundingBox(-180.0, 180.0, -90.0, 90.0);
    Geometry expResult = (new GeometryFactory()).createPoint(new Coordinate(100, 80));
    Geometry result =
        OSHDbGeometryBuilder.getGeometryClipped(entity, timestamp, areaDecider, clipBbox);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetGeometryClipped_4args_2() {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, 0L, 1L, 1, properties, 1000000000L, 800000000L);
    long timestamp = 0L;
    TagInterpreter areaDecider = null;
    Polygon clipPoly = (new BoundingBox(-180, 180, -90, 90)).getGeometry();
    Geometry expResult = (new GeometryFactory()).createPoint(new Coordinate(100, 80));
    Geometry result =
        OSHDbGeometryBuilder.getGeometryClipped(entity, timestamp, areaDecider, clipPoly);
    assertEquals(expResult, result);
  }

}
