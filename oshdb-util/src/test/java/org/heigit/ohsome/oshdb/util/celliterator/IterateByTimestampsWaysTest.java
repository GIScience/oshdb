package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
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
 * Tests the {@link CellIterator#iterateByTimestamps(OSHEntitySource)} method on OSM ways.
 */
class IterateByTimestampsWaysTest {

  private final GridOSHWays oshdbDataGridCell;
  TagInterpreter areaDecider;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * {@link GridOSHWays}.
   */
  IterateByTimestampsWaysTest() throws IOException {
    OSMXmlReader osmXmlTestData = new OSMXmlReader();
    osmXmlTestData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHWays(osmXmlTestData);
  }

  @Test
  void testGeometryChange() {
    // way: creation and two geometry changes, but no tag changes
    // way getting more nodes, one disappears

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 100,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(10, result.size());
    assertEquals(result.get(1).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertEquals(4, result.get(0).geometry().get().getNumPoints());
    assertEquals(8, result.get(1).geometry().get().getNumPoints());
    assertEquals(9, result.get(2).geometry().get().getNumPoints());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof LineString);
    Geometry geom2 = result.get(1).geometry().get();
    assertTrue(geom2 instanceof LineString);
    Geometry geom3 = result.get(2).geometry().get();
    assertTrue(geom3 instanceof LineString);
    assertEquals(31, result.get(0).osmEntity().getChangesetId());
    assertNotEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertNotEquals(result.get(2).geometry().get(), result.get(1).geometry().get());
  }

  @Test
  void testGeometryChangeOfNodeInWay() {
    // way: creation and geometry change of nodes, but no tag changes
    // way with two then 3 nodes, first two nodes changed lat lon
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 101,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(10, result.size());

    assertEquals(34, result.get(0).osmEntity().getChangesetId());
    assertEquals(35, result.get(8).osmEntity().getChangesetId());

    assertNotEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertNotEquals(result.get(2).geometry().get(), result.get(1).geometry().get());
    assertEquals(result.get(5).geometry().get(), result.get(4).geometry().get());
    assertNotEquals(result.get(9).geometry().get(), result.get(1).geometry().get());
  }

  @Test
  void testVisibleChange() {
    // way: creation and 2 visible changes, but no geometry and no tag changes
    // way visible tag changed

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 102,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(10, result.size());

    assertEquals(36, result.get(0).osmEntity().getChangesetId());
    assertEquals(38, result.get(9).osmEntity().getChangesetId());
  }

  @Test
  void testTagChange() {
    // way: creation and two tag changes, one geometry change

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 103,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(11, result.size());

    assertEquals(3, result.get(0).geometry().get().getNumPoints());
    assertEquals(5, result.get(2).geometry().get().getNumPoints());
    assertEquals(5, result.get(10).geometry().get().getNumPoints());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof LineString);
    Geometry geom2 = result.get(2).geometry().get();
    assertTrue(geom2 instanceof LineString);
    Geometry geom3 = result.get(10).geometry().get();
    assertTrue(geom3 instanceof LineString);
    assertNotEquals(result.get(2).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertNotEquals(result.get(10).osmEntity().getTags(), result.get(2).osmEntity().getTags());
    assertNotEquals(result.get(2).geometry().get(), result.get(0).geometry().get());
    assertEquals(result.get(10).geometry().get(), result.get(2).geometry().get());
  }

  @Test
  void testMultipleChangesOnNodesOfWay() {
    // way: nodes have different changes
    // node 12: tag change
    // node 13: visible change
    // node 14: multiple changes
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 104,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(8, result.size());
    assertEquals(2, result.get(0).geometry().get().getNumPoints());
    assertEquals(3, result.get(3).geometry().get().getNumPoints());
    assertEquals(2, result.get(4).geometry().get().getNumPoints());

    assertEquals(42, result.get(0).osmEntity().getChangesetId());
    assertEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertNotEquals(result.get(3).geometry().get(), result.get(1).geometry().get());
    assertNotEquals(result.get(4).geometry().get(), result.get(3).geometry().get());
  }

  @Test
  void testMultipleChanges() {
    // way and nodes have different changes
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 105,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(7, result.size());

    assertNotEquals(result.get(2).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertEquals(result.get(6).osmEntity().getTags(), result.get(2).osmEntity().getTags());
    assertEquals(result.get(1).geometry().get(), result.get(0).geometry().get());
    assertNotEquals(result.get(3).geometry().get(), result.get(1).geometry().get());
    assertNotEquals(result.get(6).geometry().get(), result.get(3).geometry().get());
  }

  @Test
  void testPolygonAreaYesTagDisappears() {
    // way seems to be polygon with area=yes, later linestring because area=yes deleted
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 106,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(11, result.size());

    assertEquals(5, result.get(0).geometry().get().getNumPoints());
    assertEquals(5, result.get(1).geometry().get().getNumPoints());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(8).geometry().get();
    assertTrue(geom2 instanceof LineString);
    assertNotEquals(result.get(8).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertNotEquals(result.get(8).geometry().get(), result.get(0).geometry().get());
  }

  @Test
  void testPolygonAreaYesNodeDisappears() {
    // way seems to be polygon with area=yes, later linestring because one node deleted
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 107,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(11, result.size());
    assertEquals(5, result.get(0).geometry().get().getNumPoints());
    assertEquals(4, result.get(8).geometry().get().getNumPoints());

    Geometry geom = result.get(0).geometry().get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(8).geometry().get();
    assertTrue(geom2 instanceof LineString);
    assertEquals(result.get(8).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertNotEquals(result.get(8).geometry().get(), result.get(0).geometry().get());
  }

  @Test
  void testTimestampInclusion() {
    // rule for contributions that fall exactly at time interval limits:
    // start timestamp: included, end timestamp: excluded
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2009-02-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 108,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(9, result.size());
  }

  @Test
  void testNodeChangeOutsideBboxIsNotGeometryChange() {
    // way: creation and one geometry change, but no tag changes
    // node 23 outside bbox with lon lat change, should not be change in geometry inside bbox
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2010-02-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(1.8, 1.3, 2.7, 2.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 110,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertEquals(2, result.size());
    assertEquals(result.get(0).geometry().get(), result.get(1).geometry().get());
  }

  @Test
  void testNodeChangeOutsideBboxAffectsPartOfLineStringInBbox() {
    // way: creation and one geometry change, but no tag changes
    // node 23 outside bbox with lon lat change, way between 24 and 25 intersects bbox
    // Node 25 outside bbox with lonlat change, way between 24 and 25 changes
    // should be geometry change
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2012-08-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(1.8, 1.3, 2.7, 2.7),
        areaDecider,
        oshEntity -> oshEntity.getId() == 110,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();

    assertNotEquals(result.get(0).geometry().get(), result.get(3).geometry().get());
    assertEquals(4, result.size());
    assertEquals(3, result.get(1).geometry().get().getNumPoints());
    assertEquals(4, result.get(0).unclippedGeometry().get().getNumPoints());
  }

  @Test
  void testNodeRefsDeletedInVersion2() {
    // way with three nodes,  node refs deleted in version 2
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 112,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        OSHEntitySource.fromGridOSHEntity(oshdbDataGridCell)
    ).toList();
    assertEquals(3, result.get(0).geometry().get().getNumPoints());
    // only 4 timestamps in result, because after 03/2012 no more node refs
    assertEquals(4, result.size());
  }
}
