package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.ohsome.oshdb.util.celliterator.helpers.GridOSHFactory;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link CellIterator#iterateByTimestamps(OSHEntitySource)} method on OSM relations.
 */
class IterateByTimestampsRelationsTest {
  private final GridOSHRelations oshdbDataGridCell;
  TagInterpreter areaDecider;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * {@link GridOSHRelations}.
   */
  IterateByTimestampsRelationsTest() throws IOException {
    OSMXmlReader osmXmlTestData = new OSMXmlReader();
    osmXmlTestData.add("./src/test/resources/different-timestamps/polygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHRelations(osmXmlTestData);
  }

  @Test
  void testGeometryChange() {
    // relation: creation and two geometry changes, but no tag changes
    // relation getting more ways, one disappears
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 500,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(12, result.size());

    assertEquals(300, result.get(0).osmEntity().getChangesetId());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof MultiPolygon);
    Geometry geom3 = result.get(1).geometry().get();
    assertTrue(geom3 instanceof MultiPolygon);
    Geometry geom4 = result.get(2).geometry().get();
    assertTrue(geom4 instanceof MultiPolygon);
  }

  @Test
  void testVisibleChange() {
    // relation: creation and 2 visible changes, but no geometry and no tag changes
    // relation visible tag changed
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 501,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(9, result.size());
    assertEquals(303, result.get(0).osmEntity().getChangesetId());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void testWaysNotExistent() {
    // relation with two ways, both missing
    assertDoesNotThrow(() -> {
      (new CellIterator(
          new OSHDBTimestamps(
              "2000-01-01T00:00:00Z",
              "2020-01-01T00:00:00Z",
              "P1Y"
          ).get(),
          OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
          areaDecider,
          oshEntity -> oshEntity.getId() == 502,
          osmEntity -> true,
          false
      )).iterateByTimestamps(
          OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
      ).collect(Collectors.toList());
    });
  }

  @Test
  void testTagChange() {
    // relation: creation and two tag changes
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 503,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(14, result.size());
    assertEquals(307, result.get(0).osmEntity().getChangesetId());
  }

  @Test
  void testGeometryChangeOfNodeRefsInWays() {
    // relation: creation and geometry change of ways, but no tag changes
    // relation, way 109 -inner- and 110 -outer- ways changed node refs-
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 504,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());
    assertEquals(310, result.get(0).osmEntity().getChangesetId());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom3 = result.get(1).geometry().get();
    assertTrue(geom3 instanceof Polygon);

    assertNotEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertNotEquals(result.get(2).geometry().get(), result.get(1).geometry().get());
    assertEquals(result.get(3).geometry().get(), result.get(2).geometry().get());
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInWay() {
    // relation: creation
    // relation, way 112 -outer- changed node coordinates
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 505,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());
    assertEquals(312, result.get(0).osmEntity().getChangesetId());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom3 = result.get(1).geometry().get();
    assertTrue(geom3 instanceof Polygon);

    assertNotEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertNotEquals(result.get(6).geometry().get(), result.get(1).geometry().get());
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInRelationAndWay() {
    // relation: creation
    // relation, with node members, nodes and nodes in way changed coordinates
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 506,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());
    assertEquals(313, result.get(0).osmEntity().getChangesetId());

    assertNotEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertEquals(result.get(6).geometry().get(), result.get(5).geometry().get());
  }

  @Test
  void testGeometryCollection() {
    // relation, not valid, should be geometryCollection
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 507,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());

    assertEquals(314, result.get(0).osmEntity().getChangesetId());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom2 = result.get(9).geometry().get();
    assertTrue(geom2 instanceof GeometryCollection);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void testNodesOfWaysNotExistent() {
    // relation 2 way members nodes do not exist
    assertDoesNotThrow(() -> {
      (new CellIterator(
          new OSHDBTimestamps(
              "2000-01-01T00:00:00Z",
              "2020-01-01T00:00:00Z",
              "P1Y"
          ).get(),
          OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
          areaDecider,
          oshEntity -> oshEntity.getId() == 508,
          osmEntity -> true,
          false
      )).iterateByTimestamps(
          OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
      ).collect(Collectors.toList());
    });
  }

  @Test
  void testVisibleChangeOfNodeInWay() {
    // relation, way member: node 52 changes visible tag
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 509,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());
    assertEquals(316, result.get(0).osmEntity().getChangesetId());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(1).geometry().get();
    assertTrue(geom2 instanceof Polygon);
    Geometry geom3 = result.get(2).geometry().get();
    assertTrue(geom3 instanceof Polygon);
    Geometry geom4 = result.get(3).geometry().get();
    assertTrue(geom4 instanceof Polygon);
    Geometry geom5 = result.get(9).geometry().get();
    assertTrue(geom5 instanceof Polygon);

    assertNotEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertEquals(result.get(2).geometry().get(), result.get(1).geometry().get());
  }

  @Test
  void testTagChangeOfNodeInWay() {
    // relation, way member: node 53 changes tags-
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 510,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(13, result.size());
    assertEquals(317, result.get(0).osmEntity().getChangesetId());
  }

  @Test
  void testVisibleChangeOfWay() {
    // relation, way member: way 119 changes visible tag-
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 511,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());
    assertEquals(318, result.get(0).osmEntity().getChangesetId());

    assertTrue(result.get(6).geometry().get().isEmpty());
  }

  @Test
  void testVisibleChangeOfOneWayOfOuterRing() {
    // relation, 2 way members making outer ring: way 120 changes visible tag later, 121 not
    // ways together making outer ring
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 512,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(7).geometry().get();
    assertTrue(geom2 instanceof GeometryCollection);
    assertEquals(319, result.get(0).osmEntity().getChangesetId());
  }

  @Test
  void testTagChangeOfWay() {
    // relation, way member: way 122 changes tags
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 513,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(12, result.size());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(2).geometry().get();
    assertTrue(geom2 instanceof Polygon);
    assertEquals(320, result.get(0).osmEntity().getChangesetId());
  }

  @Test
  void testOneOfTwoPolygonDisappears() {
    // relation, at the beginning two polygons, one disappears later
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 514,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(12, result.size());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof MultiPolygon);
    Geometry geom2 = result.get(9).geometry().get();
    assertTrue(geom2 instanceof Polygon);
    assertEquals(321, result.get(0).osmEntity().getChangesetId());
    assertNotEquals(result.get(9).geometry().get(), result.get(0).geometry().get());
  }

  @Test
  void testWaySplitUpInTwo() {
    // relation, at the beginning one way, split up later
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 515,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(12, result.size());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom1 = result.get(1).geometry().get();
    assertTrue(geom1 instanceof GeometryCollection);
    Geometry geom2 = result.get(9).geometry().get();
    assertTrue(geom2 instanceof Polygon);
    assertEquals(323, result.get(0).osmEntity().getChangesetId());
    assertNotEquals(result.get(9).geometry().get(), result.get(0).geometry().get());
  }

  @Test
  void testPolygonIntersectingDataPartly() {

    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 22.7);
    coords[2] = new Coordinate(22.7, 22.7);
    coords[3] = new Coordinate(22.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 22.7, 22.7),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(10, result.size());
  }

  @Test
  void testPolygonIntersectingDataOnlyAtBorderLine() {

    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.7, 10.4);
    coords[1] = new Coordinate(10.94, 10.4);
    coords[2] = new Coordinate(10.94, 10.9);
    coords[3] = new Coordinate(10.7, 10.9);
    coords[4] = new Coordinate(10.7, 10.4);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(0, result.size());
  }

  @Test
  void testPolygonIntersectingDataCompletely() {

    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 52.7);
    coords[2] = new Coordinate(52.7, 52.7);
    coords[3] = new Coordinate(52.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 52.7, 52.7),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(10, result.size());
  }

  @Test
  void testPolygonNotIntersectingData() {

    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(48, 49);
    coords[1] = new Coordinate(48, 50);
    coords[2] = new Coordinate(49, 50);
    coords[3] = new Coordinate(49, 49);
    coords[4] = new Coordinate(48, 49);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> resultPoly = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(50.0, 51.0, 51.0, 52.0),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertTrue(resultPoly.isEmpty());
  }

  @Test
  void testNodeChangeOutsideBbox() {
    // relation: 2 ways, each has 5 points, making polygon
    // nodes outside bbox have lon lat change in 2009 and 2011, the latest one affects geometry of
    // polygon inside bbox
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2019-08-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 22.7, 22.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertNotEquals(result.get(3).geometry().get(), result.get(0).geometry().get());
  }

  @Test
  void testPolygonIntersectingDataCompletelyTimeIntervalAfterChanges() {

    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 52.7);
    coords[2] = new Coordinate(52.7, 52.7);
    coords[3] = new Coordinate(52.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 517,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
  }

  @Test
  void testTimeIntervalAfterChanges() {

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 52.7, 52.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 517,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(3, result.size());
  }

  @Test
  void testBboxOutsidePolygon() {

    List<IterateByTimestampEntry> resultPoly = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(50.0, 50.0, 52.0, 52.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertTrue(resultPoly.isEmpty());
  }

  @Test
  void testUnclippedGeom() {
    // relation: 2 ways, each has 5 points, making 1 polygon
    // geometry change of nodes of relation 2009 and 2011
    // OSHDBBoundingBox covers only left side of polygon
    // unclipped geom != clipped geom
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2019-08-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 22.7, 22.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    // geom of requested area vs full geom after modification
    assertNotEquals(result.get(0).geometry().get().getArea(),
        result.get(0).unclippedGeometry().get().getArea());
    // full geom changed
    assertNotEquals(result.get(2).unclippedGeometry().get().getArea(),
        result.get(0).unclippedGeometry().get().getArea());
    assertNotEquals(result.get(2).unclippedGeometry().get().getArea(),
        result.get(4).unclippedGeometry().get().getArea());
  }

  @Test
  void testSelfIntersectingPolygonClipped() {
    // Polygon with self crossing way
    // partly intersected by bbox polygon
    // happy if it works without crashing
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(7.31, 1.0);
    coords[1] = new Coordinate(7.335, 1.0);
    coords[2] = new Coordinate(7.335, 2.0);
    coords[3] = new Coordinate(7.31, 2.0);
    coords[4] = new Coordinate(7.31, 1.0);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 520,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertTrue(result.isEmpty());
  }

  @Test
  void testMembersDisappear() {
    // relation with one way member(nodes of way have changes in 2009 and 2011), in version 2 member
    // is deleted
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 521,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
  }

  @Test
  void testTimeIntervalAfterDeletionInVersion2() {
    // relation in second version visible = false, time interval includes version 3
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 522,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(5, result.size());
  }

  @Test
  void testTimeIntervalAfterDeletionInCurrentVersion() {
    // relation in first and third version visible = false, time interval includes version 3
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 523,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(0, result.size());
  }

  @Test
  void testMembersDisappearClipped() {
    // relation with one way member(nodes of way have changes in 2009 and 2011), in version 2 member
    // is deleted
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 22.7);
    coords[2] = new Coordinate(22.7, 22.7);
    coords[3] = new Coordinate(22.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 521,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
  }

  @Test
  void testTimeIntervalAfterDeletionInVersion2Clipped() {
    // relation in second version visible = false, time interval includes version 3
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 22.7);
    coords[2] = new Coordinate(22.7, 22.7);
    coords[3] = new Coordinate(22.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 522,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(5, result.size());
  }

  @Test
  void testTimeIntervalAfterDeletionInCurrentVersionClipped() {
    // relation in first and third version visible = false, time interval includes version 3
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 22.7);
    coords[2] = new Coordinate(22.7, 22.7);
    coords[3] = new Coordinate(22.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 523,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(0, result.size());
  }

  @Test
  void testExcludingVersion2Clipped() {
    // relation in second version visible = false, time interval includes version 3
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(7.31, 1.0);
    coords[1] = new Coordinate(7.335, 1.0);
    coords[2] = new Coordinate(7.335, 2.0);
    coords[3] = new Coordinate(7.31, 2.0);
    coords[4] = new Coordinate(7.31, 1.0);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2012-01-01T00:00:00Z",
            "2014-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 500,
        osmEntity -> !(osmEntity.getVersion() == 2),
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
  }

  @Test
  void testClippingPolygonIsVeryBig() {
    // relation with two way members(nodes of ways have changes in 2009 and 2011)
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(-180, -90);
    coords[1] = new Coordinate(180, -90);
    coords[2] = new Coordinate(180, 90);
    coords[3] = new Coordinate(-180, 90);
    coords[4] = new Coordinate(-180, -90);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2008-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(12, result.size());
  }

}
