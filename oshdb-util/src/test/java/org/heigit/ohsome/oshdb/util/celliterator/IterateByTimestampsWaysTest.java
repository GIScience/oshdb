package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.ohsome.oshdb.util.celliterator.helpers.GridOSHFactory;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link CellIterator#iterateByTimestamps(GridOSHEntity)} method on OSM ways.
 */
public class IterateByTimestampsWaysTest {

  private final GridOSHWays oshdbDataGridCell;
  TagInterpreter areaDecider;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * {@link GridOSHWays}.
   */
  public IterateByTimestampsWaysTest() throws IOException {
    OSMXmlReader osmXmlTestData = new OSMXmlReader();
    osmXmlTestData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHWays(osmXmlTestData);
  }

  @Test
  public void testGeometryChange() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(10, result.size());
    assertEquals(result.get(1).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertEquals(4, result.get(0).geometry.get().getNumPoints());
    assertEquals(8, result.get(1).geometry.get().getNumPoints());
    assertEquals(9, result.get(2).geometry.get().getNumPoints());

    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof LineString);
    Geometry geom2 = result.get(1).geometry.get();
    assertTrue(geom2 instanceof LineString);
    Geometry geom3 = result.get(2).geometry.get();
    assertTrue(geom3 instanceof LineString);
    assertEquals(31, result.get(0).osmEntity.getChangesetId());
    assertNotEquals(result.get(1).geometry.get(), result.get(0).geometry.get());
    assertNotEquals(result.get(2).geometry.get(), result.get(1).geometry.get());
  }

  @Test
  public void testGeometryChangeOfNodeInWay() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(10, result.size());

    assertEquals(34, result.get(0).osmEntity.getChangesetId());
    assertEquals(35, result.get(8).osmEntity.getChangesetId());

    assertNotEquals(result.get(1).geometry.get(), result.get(0).geometry.get());
    assertNotEquals(result.get(2).geometry.get(), result.get(1).geometry.get());
    assertEquals(result.get(5).geometry.get(), result.get(4).geometry.get());
    assertNotEquals(result.get(9).geometry.get(), result.get(1).geometry.get());
  }

  @Test
  public void testVisibleChange() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(10, result.size());

    assertEquals(36, result.get(0).osmEntity.getChangesetId());
    assertEquals(38, result.get(9).osmEntity.getChangesetId());
  }

  @Test
  public void testTagChange() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(11, result.size());

    assertEquals(3, result.get(0).geometry.get().getNumPoints());
    assertEquals(5, result.get(2).geometry.get().getNumPoints());
    assertEquals(5, result.get(10).geometry.get().getNumPoints());

    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof LineString);
    Geometry geom2 = result.get(2).geometry.get();
    assertTrue(geom2 instanceof LineString);
    Geometry geom3 = result.get(10).geometry.get();
    assertTrue(geom3 instanceof LineString);
    assertNotEquals(result.get(2).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertNotEquals(result.get(10).osmEntity.getRawTags(), result.get(2).osmEntity.getRawTags());
    assertNotEquals(result.get(2).geometry.get(), result.get(0).geometry.get());
    assertEquals(result.get(10).geometry.get(), result.get(2).geometry.get());
  }

  @Test
  public void testMultipleChangesOnNodesOfWay() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(8, result.size());
    assertEquals(2, result.get(0).geometry.get().getNumPoints());
    assertEquals(3, result.get(3).geometry.get().getNumPoints());
    assertEquals(2, result.get(4).geometry.get().getNumPoints());

    assertEquals(42, result.get(0).osmEntity.getChangesetId());
    assertEquals(result.get(1).geometry.get(), result.get(0).geometry.get());
    assertNotEquals(result.get(3).geometry.get(), result.get(1).geometry.get());
    assertNotEquals(result.get(4).geometry.get(), result.get(3).geometry.get());
  }

  @Test
  public void testMultipleChanges() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(7, result.size());

    assertNotEquals(result.get(2).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertEquals(result.get(6).osmEntity.getRawTags(), result.get(2).osmEntity.getRawTags());
    assertEquals(result.get(1).geometry.get(), result.get(0).geometry.get());
    assertNotEquals(result.get(3).geometry.get(), result.get(1).geometry.get());
    assertNotEquals(result.get(6).geometry.get(), result.get(3).geometry.get());
  }

  @Test
  public void testPolygonAreaYesTagDisappears() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(11, result.size());

    assertEquals(5, result.get(0).geometry.get().getNumPoints());
    assertEquals(5, result.get(1).geometry.get().getNumPoints());

    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(8).geometry.get();
    assertTrue(geom2 instanceof LineString);
    assertNotEquals(result.get(8).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertNotEquals(result.get(8).geometry.get(), result.get(0).geometry.get());
  }

  @Test
  public void testPolygonAreaYesNodeDisappears() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(11, result.size());
    assertEquals(5, result.get(0).geometry.get().getNumPoints());
    assertEquals(4, result.get(8).geometry.get().getNumPoints());

    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof Polygon);
    Geometry geom2 = result.get(8).geometry.get();
    assertTrue(geom2 instanceof LineString);
    assertEquals(result.get(8).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertNotEquals(result.get(8).geometry.get(), result.get(0).geometry.get());
  }

  @Test
  public void testTimestampInclusion() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(9, result.size());
  }

  @Test
  public void testNodeChangeOutsideBboxIsNotGeometryChange() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(2, result.size());
    assertEquals(result.get(0).geometry.get(), result.get(1).geometry.get());
  }

  @Test
  public void testNodeChangeOutsideBboxAffectsPartOfLineStringInBbox() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertNotEquals(result.get(0).geometry.get(), result.get(3).geometry.get());
    assertEquals(4, result.size());
    assertEquals(3, result.get(1).geometry.get().getNumPoints());
    assertEquals(4, result.get(0).unclippedGeometry.get().getNumPoints());
  }

  @Test
  public void testNodeRefsDeletedInVersion2() {
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
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(3, result.get(0).geometry.get().getNumPoints());
    // only 4 timestamps in result, because after 03/2012 no more node refs
    assertEquals(4, result.size());
  }
}
