package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.ohsome.oshdb.util.celliterator.helpers.GridOSHFactory;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link CellIterator#iterateByContribution(OSHEntitySource)} method on ways.
 */
class IterateByContributionWaysTest {
  private GridOSHWays oshdbDataGridCell;
  private final OSMXmlReader osmXmlTestData = new OSMXmlReader();
  TagInterpreter areaDecider;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * {@link GridOSHWays}.
   */
  IterateByContributionWaysTest() throws IOException {
    osmXmlTestData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHWays(osmXmlTestData);
  }

  @Test
  void testGeometryChange() {
    // way: creation and two geometry changes, but no tag changes
    // way getting more nodes, one disappears
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 100,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities().get()
    );
    assertEquals(31, result.get(0).changeset());

    assertEquals(4, result.get(0).geometry().get().getNumPoints());
    assertEquals(8, result.get(1).geometry().get().getNumPoints());
    assertEquals(9, result.get(2).geometry().get().getNumPoints());

    assertNull(result.get(0).previousGeometry().get());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof LineString);
    Geometry geom2 = result.get(1).geometry().get();
    assertTrue(geom2 instanceof LineString);
    Geometry geom3 = result.get(2).geometry().get();
    assertTrue(geom3 instanceof LineString);

    assertNotEquals(result.get(1).geometry().get(), result.get(1).previousGeometry().get());
    assertNotEquals(result.get(2).geometry().get(), result.get(2).previousGeometry().get());
  }

  @Test
  void testGeometryChangeOfNodeInWay() {
    // way: creation and geometry change of nodes, but no tag changes
    // way with two then 3 nodes, first two nodes changed lat lon
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 101,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(4, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities().get()
    );

    assertEquals(34, result.get(0).changeset());

    assertEquals(2, result.get(0).geometry().get().getNumPoints());
    assertEquals(2, result.get(1).geometry().get().getNumPoints());
    assertEquals(3, result.get(3).geometry().get().getNumPoints());

    assertNull(result.get(0).previousGeometry().get());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof LineString);
    Geometry geom3 = result.get(1).geometry().get();
    assertTrue(geom3 instanceof LineString);
    Geometry geom4 = result.get(3).geometry().get();
    assertTrue(geom4 instanceof LineString);

    assertNotEquals(result.get(1).geometry().get(), result.get(1).previousGeometry().get());
    assertNotEquals(result.get(2).geometry().get(), result.get(2).previousGeometry().get());
    assertNotEquals(result.get(3).geometry().get(), result.get(3).previousGeometry().get());
  }

  @Test
  void testVisibleChange() {
    // way: creation and 2 visible changes, but no geometry and no tag changes
    // way visible tag changed
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 102,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(2).activities().get()
    );
    assertEquals(36, result.get(0).changeset());
  }

  @Test
  void testTagChange() {
    // way: creation and two tag changes, one geometry change
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2020-01-01T00:00:01Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 103,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE, ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE),
        result.get(2).activities().get()
    );
    assertEquals(39, result.get(0).changeset());

    assertEquals(3, result.get(0).geometry().get().getNumPoints());
    assertEquals(5, result.get(1).geometry().get().getNumPoints());
    assertEquals(5, result.get(2).geometry().get().getNumPoints());

    assertNull(result.get(0).previousGeometry().get());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof LineString);
    Geometry geom2 = result.get(1).geometry().get();
    assertTrue(geom2 instanceof LineString);
    Geometry geom3 = result.get(2).geometry().get();
    assertTrue(geom3 instanceof LineString);

    assertNotEquals(result.get(1).geometry().get(), result.get(1).previousGeometry().get());
    assertEquals(result.get(2).geometry().get(), result.get(2).previousGeometry().get());
  }

  @Test
  void testMultipleChangesOnNodesOfWay() {
    // way: nodes have different changes
    // node 12: tag change
    // node 13: visible change
    // node 14: multiple changes
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:01Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 104,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(6, result.size());
    assertEquals(2, result.get(0).geometry().get().getNumPoints());
    assertEquals(3, result.get(1).geometry().get().getNumPoints());
    assertEquals(2, result.get(2).geometry().get().getNumPoints());
    assertEquals(3, result.get(3).geometry().get().getNumPoints());
    assertEquals(3, result.get(4).geometry().get().getNumPoints());

    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(3).activities().get()
    );
    assertEquals(42, result.get(0).changeset());
  }


  @Test
  void testMultipleChanges() {
    // way and nodes have different changes
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:01Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 105,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(6, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE, ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(3).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(4).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(5).activities().get()
    );

    assertEquals(44, result.get(0).changeset());
    assertNotEquals(result.get(1).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertEquals(result.get(5).osmEntity().getTags(), result.get(3).osmEntity().getTags());
  }

  @Test
  void testPolygonAreaYesTagDisappears() {
    // way seems to be polygon with area=yes, later linestring because area=yes deleted
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:01Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 106,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(2, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE, ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );

    assertEquals(48, result.get(0).changeset());

    assertEquals(5, result.get(0).geometry().get().getNumPoints());
    assertEquals(5, result.get(1).geometry().get().getNumPoints());

    assertNull(result.get(0).previousGeometry().get());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(1).geometry().get();
    assertTrue(geom2 instanceof LineString);

    assertNotEquals(result.get(1).geometry().get(), result.get(1).previousGeometry().get());
  }

  @Test
  void testPolygonAreaYesNodeDisappears() {
    // way seems to be polygon with area=yes, later linestring because one node deleted
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:01Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 107,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(2, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );

    assertEquals(50, result.get(0).changeset());

    assertEquals(5, result.get(0).geometry().get().getNumPoints());
    assertEquals(4, result.get(1).geometry().get().getNumPoints());

    assertNull(result.get(0).previousGeometry().get());
    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(1).geometry().get();
    assertTrue(geom2 instanceof LineString);

    assertNotEquals(result.get(1).geometry().get(), result.get(1).previousGeometry().get());
  }

  @Test
  void testTimestampInclusion() {
    // rule for contributions that fall exactly at time interval limits:
    // start timestamp: included, end timestamp: excluded
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2009-02-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 108,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    // should be 2: entity has created at start time, modified in between and at end time (excluded)
    assertEquals(2, result.size());
    assertEquals(61, result.get(0).changeset());
  }

  @Test
  void testTwoNodesChangedAtSameTimeDifferentChangesets() {
    // way with two nodes, nodes changed lat lon, both at same time, different changesets
    // which changeset is shown in result.get(1).changeset()? -> from node 20, not 21
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 109,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(2, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    // randomly 332 or 334
    //assertEquals(332, result.get(1).changeset());
  }

  @Test
  void testNodeChangeOutsideBboxIsNotGeometryChange() {
    // way: creation and one geometry change, but no tag changes
    // node 23 outside bbox with lon lat change
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2009-08-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(1.8, 1.3, 2.7, 2.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 110,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(2, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertTrue(result.get(1).activities().get().isEmpty());
    assertEquals(3, result.get(1).geometry().get().getNumPoints());
  }

  @Test
  void testNodeChangeOutsideBboxAffectsPartOfLineStringInBbox() {
    // way: creation and one geometry change, but no tag changes
    // node 23 outside bbox with lon lat change, way between 24 and 25 intersects bbox
    // Node 25 outside bbox with lonlat change, way between 24 and 25 changes
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2012-08-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(1.8, 1.3, 2.7, 2.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 110,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertTrue(result.get(1).activities().get().isEmpty());
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities().get()
    );
    assertEquals(3, result.get(1).geometry().get().getNumPoints());
  }

  @Test
  void testTagChangeOfNodeInWay() {
    // way: creation and geometry change of nodes, but no tag changes
    // way with two then 3 nodes, first two nodes changed lat lon
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 111,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertTrue(result.get(1).activities().get().isEmpty());
    assertTrue(result.get(2).activities().get().isEmpty());
  }

  @Test
  void testNodeRefsDeletedInVersion2() {
    // way with three nodes,  node refs deleted in version 2
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 112,
        osmEntity -> true,
        false
    )).iterateByContribution(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities().get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(2).activities().get()
    );
  }
}
