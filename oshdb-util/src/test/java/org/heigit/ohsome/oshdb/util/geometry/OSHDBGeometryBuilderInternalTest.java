package org.heigit.ohsome.oshdb.util.geometry;

import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.GEOM_PRECISION_TO_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilderInternal.AuxiliaryData;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaAlways;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaMultipolygonAllOuters;
import org.heigit.ohsome.oshdb.util.geometry.helpers.FakeTagInterpreterAreaNever;
import org.heigit.ohsome.oshdb.util.geometry.helpers.TimestampParser;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * Tests the {@link OSHDBGeometryBuilderInternal} class.
 */
class OSHDBGeometryBuilderInternalTest extends OSHDBGeometryTest {
  private static final double DELTA = 1E-6;

  private static final OSHDBTimestamp t1 = TimestampParser.toOSHDBTimestamp("2000-01-01");
  private static final OSMNode n1 = OSM.node(1, 1, t1.getEpochSecond(), 1, 0, new int[] {},
      (int) (100 * GEOM_PRECISION_TO_LONG), (int) (80 * GEOM_PRECISION_TO_LONG));
  private static final OSMNode n2 = OSM.node(2, 1, t1.getEpochSecond(), 1, 0, new int[] {},
      (int) (110 * GEOM_PRECISION_TO_LONG), (int) (80.1 * GEOM_PRECISION_TO_LONG));
  private static final OSMNode n3 = OSM.node(3, 1, t1.getEpochSecond(), 1, 0, new int[] {},
      (int) (110 * GEOM_PRECISION_TO_LONG), (int) (81.1 * GEOM_PRECISION_TO_LONG));
  private static final OSMNode n4 = OSM.node(4, 1, t1.getEpochSecond(), 1, 0, new int[] {},
      (int) (100 * GEOM_PRECISION_TO_LONG), (int) (81.1 * GEOM_PRECISION_TO_LONG));
  private static final OSMWay w2 = OSM.way(1, 1, t1.getEpochSecond(), 1, 0, new int[] {},
      new OSMMember[] {
          new OSMMember(1, OSMType.NODE, -1),
          new OSMMember(2, OSMType.NODE, -1),
          new OSMMember(3, OSMType.NODE, -1),
          new OSMMember(4, OSMType.NODE, -1),
          new OSMMember(1, OSMType.NODE, -1)
      });
  private static final OSMRelation r1 = OSM.relation(1, 1, t1.getEpochSecond(), 1, 0, new int[] {},
      new OSMMember[] {
          new OSMMember(2, OSMType.WAY, 1)
      });

  public OSHDBGeometryBuilderInternalTest() {
    super(
        "./src/test/resources/geometryBuilder.osh"
    );
  }


  @Nested
  class Node {
    private final OSHDBGeometryBuilderInternal geometryBuilder =
            new OSHDBGeometryBuilderInternal(areaDecider);

    @Test
    void testNodeGetGeometryAuxiliary() {
      Geometry result = geometryBuilder.getGeometry(n1, new AuxiliaryData(List.of(), List.of()));
      assertTrue(result instanceof Point);
      assertEquals(100, result.getCoordinates()[0].x, DELTA);
      assertEquals(80, result.getCoordinates()[0].y, DELTA);
    }
  }


  @Nested
  class Way {
    private final OSHDBGeometryBuilderInternal geometryBuilderLines =
            new OSHDBGeometryBuilderInternal(new FakeTagInterpreterAreaNever());
    private final OSHDBGeometryBuilderInternal geometryBuilderAreas =
            new OSHDBGeometryBuilderInternal(new FakeTagInterpreterAreaAlways());
    AuxiliaryData aux = new AuxiliaryData(List.of(n1, n2, n3, n4, n1), null);

    @Nested
    class GetGeometry {
      @Test
      void testWayGetGeometryLineString() {
        Geometry result = geometryBuilderLines.getGeometry(w2, aux);
        assertEquals("LineString", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
        assertEquals(100, result.getCoordinates()[0].x, DELTA);
        assertEquals(80, result.getCoordinates()[0].y, DELTA);
        assertEquals(110, result.getCoordinates()[2].x, DELTA);
        assertEquals(81.1, result.getCoordinates()[2].y, DELTA);
      }

      @Test
      void testWayGetGeometryPolygon() {
        Geometry result = geometryBuilderAreas.getGeometry(w2, aux);
        assertEquals("Polygon", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
        assertEquals(result.getCoordinates()[0].x, result.getCoordinates()[4].x, DELTA);
        assertEquals(result.getCoordinates()[0].y, result.getCoordinates()[4].y, DELTA);
      }
    }

    @Nested
    class GetWayGeometry {
      @Test
      void testWayGetWayGeometryAuxiliaryDataLineString() {
        Geometry result = geometryBuilderLines.getWayGeometry(w2, aux);
        assertEquals("LineString", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
        assertEquals(100, result.getCoordinates()[0].x, DELTA);
        assertEquals(80, result.getCoordinates()[0].y, DELTA);
        assertEquals(110, result.getCoordinates()[2].x, DELTA);
        assertEquals(81.1, result.getCoordinates()[2].y, DELTA);
      }

      @Test
      void testWayGetWayGeometryAuxiliaryDataPolygon() {
        Geometry result = geometryBuilderAreas.getWayGeometry(w2, aux);
        assertEquals("Polygon", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
        assertEquals(result.getCoordinates()[0].x, result.getCoordinates()[4].x, DELTA);
        assertEquals(result.getCoordinates()[0].y, result.getCoordinates()[4].y, DELTA);
      }

      @Test
      void testWayGetWayGeometryDefaultLineString() {
        OSMWay way = ways(2L, 0);
        Geometry result = geometryBuilderLines.getWayGeometry(way, t1);
        assertEquals("LineString", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
        assertEquals(100, result.getCoordinates()[0].x, DELTA);
        assertEquals(80, result.getCoordinates()[0].y, DELTA);
        assertEquals(110, result.getCoordinates()[2].x, DELTA);
        assertEquals(81.1, result.getCoordinates()[2].y, DELTA);
      }

      @Test
      void testWayGetWayGeometryDefaultPolygon() {
        OSMWay way = ways(2L, 0);
        Geometry result = geometryBuilderAreas.getWayGeometry(way, t1);
        assertEquals("Polygon", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
        assertEquals(result.getCoordinates()[0].x, result.getCoordinates()[4].x, DELTA);
        assertEquals(result.getCoordinates()[0].y, result.getCoordinates()[4].y, DELTA);
      }
    }
  }


  @Nested
  class Relation {
    private final OSHDBGeometryBuilderInternal geometryBuilderMultiPolygons =
            new OSHDBGeometryBuilderInternal(new FakeTagInterpreterAreaMultipolygonAllOuters());
    private final OSHDBGeometryBuilderInternal geometryBuilderGeometryCollections =
            new OSHDBGeometryBuilderInternal(new FakeTagInterpreterAreaNever());
    AuxiliaryData aux = new AuxiliaryData(
        List.of(w2),
        List.of(List.of(n1, n2, n3, n4, n1)));

    @Nested
    class GetGeometry {
      @Test
      void testRelationGetGeometryPolygon() {
        Geometry result = geometryBuilderMultiPolygons.getGeometry(r1, aux);
        assertEquals("Polygon", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
      }

      @Test
      void testRelationGetGeometryOther() {
        Geometry result = geometryBuilderGeometryCollections.getGeometry(r1, aux);
        assertEquals("GeometryCollection", result.getGeometryType());
        assertEquals(1, result.getNumGeometries());
        assertEquals(5, result.getGeometryN(0).getNumPoints());
      }
    }

    @Nested
    class GetMultiPolygonGeometry {
      @Test
      void testRelationGetMultiPolygonGeometryAuxiliary() {
        Geometry result = geometryBuilderMultiPolygons.getMultiPolygonGeometry(r1, aux);
        assertEquals("Polygon", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
      }

      @Test
      void testRelationGetMultiPolygonGeometryTimestamp() {
        OSMRelation relation = relations(1L, 0);
        Geometry result = geometryBuilderMultiPolygons.getMultiPolygonGeometry(relation, t1);
        assertEquals("Polygon", result.getGeometryType());
        assertEquals(5, result.getNumPoints());
      }
    }

    @Nested
    class GetGeometryCollectionGeometry {
      @Test
      void testRelationGetGeometryCollectionGeometryAuxiliary() {
        Geometry result =
                geometryBuilderGeometryCollections.getGeometryCollectionGeometry(r1, aux);
        assertEquals("GeometryCollection", result.getGeometryType());
        assertEquals(1, result.getNumGeometries());
        assertEquals(5, result.getGeometryN(0).getNumPoints());
      }

      @Test
      void testRelationGetGeometryCollectionGeometryTimestamp() {
        OSMRelation relation = relations(1L, 0);
        Geometry result =
                geometryBuilderGeometryCollections.getGeometryCollectionGeometry(relation, t1);
        assertEquals("GeometryCollection", result.getGeometryType());
        assertEquals(1, result.getNumGeometries());
        assertEquals(5, result.getGeometryN(0).getNumPoints());
      }
    }
  }
}
