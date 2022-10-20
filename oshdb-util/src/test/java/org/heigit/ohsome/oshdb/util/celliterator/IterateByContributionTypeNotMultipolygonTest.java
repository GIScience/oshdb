package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
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
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link CellIterator#iterateByContribution(GridOSHEntity)} method on relations except
 * multipolygon relations.
 */
class IterateByContributionTypeNotMultipolygonTest {
  private GridOSHRelations oshdbDataGridCell;
  private final OSMXmlReader osmXmlTestData = new OSMXmlReader();
  TagInterpreter areaDecider;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * {@link GridOSHRelations}.
   */
  IterateByContributionTypeNotMultipolygonTest() throws IOException {
    osmXmlTestData.add("./src/test/resources/different-timestamps/type-not-multipolygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHRelations(osmXmlTestData);
  }

  @Test
  void testGeometryChange() {
    // relation: creation and two geometry changes, but no tag changes
    // relation getting more ways, one disappears
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 500,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities.get()
    );
    assertEquals(300, result.get(0).changeset);
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom3 = result.get(1).geometry.get();
    assertTrue(geom3 instanceof GeometryCollection);
    Geometry geom4 = result.get(2).geometry.get();
    assertTrue(geom4 instanceof GeometryCollection);
  }

  @Test
  void testVisibleChange() {
    // relation: creation and 2 visible changes, but no geometry and no tag changes
    // relation visible tag changed
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 501,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(2).activities.get()
    );
    assertEquals(303, result.get(0).changeset);
  }

  @Test
  void testWaysNotExistent() {
    // relation with two ways, both missing
    assertDoesNotThrow(() -> {
      (new CellIterator(
          new OSHDBTimestamps(
              "2000-01-01T00:00:00Z",
              "2020-01-01T00:00:00Z"
          ).get(),
          OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
          areaDecider,
          oshEntity -> oshEntity.getId() == 502,
          osmEntity -> true,
          false
      )).iterateByContribution(
          oshdbDataGridCell
      ).collect(Collectors.toList());
    });
  }

  @Test
  void testTagChange() {
    // relation: creation and two tag changes
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 503,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE),
        result.get(2).activities.get()
    );
    assertEquals(307, result.get(0).changeset);
  }

  @Test
  void testGeometryChangeOfNodeRefsInWays() {
    // relation: creation and geometry change of ways, but no tag changes
    // relation, way 109 -inner- and 110 -outer- ways changed node refs-
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 504,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(8, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );

    assertEquals(310, result.get(0).changeset);

    assertEquals(null, result.get(0).previousGeometry.get());
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom3 = result.get(1).geometry.get();
    assertTrue(geom3 instanceof GeometryCollection);

    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
    assertEquals(result.get(2).geometry.get(), result.get(2).previousGeometry.get());
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInWay() {
    // relation: creation
    // relation, way 112 -outer- changed node coordinates
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 505,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );

    assertEquals(312, result.get(0).changeset);

    assertEquals(null, result.get(0).previousGeometry.get());
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom3 = result.get(1).geometry.get();
    assertTrue(geom3 instanceof GeometryCollection);

    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
    assertNotEquals(result.get(2).geometry.get(), result.get(2).previousGeometry.get());
  }

  @Test
  void testGeometryChangeOfNodeCoordinatesInRelationAndWay() {
    // relation: creation
    // relation, with node members, nodes and nodes in way changed coordinates
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 506,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );

    assertEquals(313, result.get(0).changeset);

    assertEquals(null, result.get(0).previousGeometry.get());
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom3 = result.get(1).geometry.get();
    assertTrue(geom3 instanceof GeometryCollection);

    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
    assertNotEquals(result.get(2).geometry.get(), result.get(2).previousGeometry.get());
  }

  @Test
  void testGeometryCollection() {
    // relation, not valid, should be geometryCollection
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 507,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );

    assertEquals(314, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom1 = result.get(1).geometry.get();
    assertTrue(geom1 instanceof GeometryCollection);
    Geometry geom2 = result.get(2).geometry.get();
    assertTrue(geom2 instanceof GeometryCollection);
  }

  @Test
  void testNodesOfWaysNotExistent() {
    // relation 2 way members nodes do not exist
    assertDoesNotThrow(() -> {
      (new CellIterator(
          new OSHDBTimestamps(
              "2000-01-01T00:00:00Z",
              "2020-01-01T00:00:00Z"
          ).get(),
          OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
          areaDecider,
          oshEntity -> oshEntity.getId() == 508,
          osmEntity -> true,
          false
      )).iterateByContribution(
          oshdbDataGridCell
      ).collect(Collectors.toList());
    });
  }

  @Test
  void testVisibleChangeOfNodeInWay() {
    // relation, way member: node 52 changes visible tag
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 509,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(5, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(3).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(4).activities.get()
    );

    assertEquals(316, result.get(0).changeset);

    assertEquals(null, result.get(0).previousGeometry.get());
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom2 = result.get(1).geometry.get();
    assertTrue(geom2 instanceof GeometryCollection);
    Geometry geom3 = result.get(2).geometry.get();
    assertTrue(geom3 instanceof GeometryCollection);
    Geometry geom4 = result.get(3).geometry.get();
    assertTrue(geom4 instanceof GeometryCollection);
    Geometry geom5 = result.get(4).geometry.get();
    assertTrue(geom5 instanceof GeometryCollection);

    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
    assertNotEquals(result.get(2).geometry.get(), result.get(2).previousGeometry.get());
  }

  @Test
  void testTagChangeOfNodeInWay() {
    // relation, way member: node 53 changes tags-
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 510,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(4, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );

    assertEquals(317, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
  }

  @Test
  void testVisibleChangeOfWay() {
    // relation, way member: way 119 changes visible tag-
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 511,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );

    assertEquals(318, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
  }

  @Test
  void testVisibleChangeOfOneWayOfOuterRing() {
    // relation, 2 way members making outer ring: way 120 changes visible tag later, 121 not
    // ways together making outer ring
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 512,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(2, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom2 = result.get(1).geometry.get();
    assertTrue(geom2 instanceof GeometryCollection);
    assertEquals(319, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
  }

  @Test
  void testTagChangeOfWay() {
    // relation, way member: way 122 changes tags
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 513,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom2 = result.get(2).geometry.get();
    assertTrue(geom2 instanceof GeometryCollection);
    assertEquals(320, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
  }

  @Test
  void testOneOfTwoPolygonDisappears() {
    // relation, at the beginning two polygons, one disappears later
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 514,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(2, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom2 = result.get(1).geometry.get();
    assertTrue(geom2 instanceof GeometryCollection);
    assertEquals(321, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
  }

  @Test
  void testWaySplitUpInTwo() {
    // relation, at the beginning one way, split up later
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 515,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities.get()
    );
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof GeometryCollection);
    Geometry geom1 = result.get(1).geometry.get();
    assertTrue(geom1 instanceof GeometryCollection);
    Geometry geom2 = result.get(2).geometry.get();
    assertTrue(geom2 instanceof GeometryCollection);
    assertEquals(323, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
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

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(3, result.size());
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

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(3, result.size());
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

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  void testNodeChangeOutsideBbox() {
    // relation: 2 ways, each has 5 points, making polygon
    // nodes outside bbox have lon lat change in 2009 and 2011, the latest one affects geometry of
    // polygon inside bbox
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2019-08-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 22.7, 22.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertTrue(result.get(1).activities.get().isEmpty());
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities.get()
    );
    assertEquals(7, result.get(1).geometry.get().getNumPoints());
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

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 517,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  void testTimeIntervalAfterChanges() {

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 52.7, 52.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 517,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  void testBboxOutsidePolygon() {
    // OSM Polygon coordinates between: minLon 10, maxLon 41, minLat 10, maxLat 45
    // OSHDBBoundingBox outside this coordinates

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(50.0, 50.0, 52.0, 52.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 516,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  void testSelfIntersectingPolygonClipped() {
    // Polygon with self crossing way
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(7.31, 1.0);
    coords[1] = new Coordinate(7.335, 1.0);
    coords[2] = new Coordinate(7.335, 2.0);
    coords[3] = new Coordinate(7.31, 2.0);
    coords[4] = new Coordinate(7.31, 1.0);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 520,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    Geometry geom1 = result.get(0).geometry.get();
    assertTrue(geom1 instanceof GeometryCollection);
    assertEquals(1, result.size());
  }

  @Test
  void testMembersDisappear() {
    // relation with one way member(nodes of way have changes in 2009 and 2011), in version 2 member
    // is deleted
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 22.7, 22.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 521,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(4, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
  }

  @Test
  void testMembersDisappearAndPreviousIsNull() {
    // relation in last version without members, previous version visible=false
    // time interval includes only last version
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2012-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(10.8, 10.3, 22.7, 22.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 522,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
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

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2016-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 523,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(1, result.size());
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(0).activities.get()
    );
  }
}
