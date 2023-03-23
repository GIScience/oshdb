package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.index.XYGrid;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.ohsome.oshdb.util.celliterator.helpers.GridOSHFactory;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests the {@link CellIterator#iterateByTimestamps(GridOSHEntity)} method on OSM nodes.
 */
class IterateByTimestampsNodesTest {
  private final GridOSHNodes oshdbDataGridCell;
  private final OSMXmlReader osmXmlTestData = new OSMXmlReader();
  TagInterpreter areaDecider;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * {@link GridOSHNodes}.
   */
  IterateByTimestampsNodesTest() throws IOException {
    osmXmlTestData.add("./src/test/resources/different-timestamps/node.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHNodes(osmXmlTestData);
  }


  @Test
  void testGeometryChange() {
    // node 1: creation and two geometry changes, but no tag changes

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 1,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();

    assertEquals(11, result.size());
    assertNotEquals(result.get(1).geometry().get().getCoordinates(),
        result.get(0).geometry().get().getCoordinates());
    assertNotEquals(result.get(2).geometry().get().getCoordinates(),
        result.get(1).geometry().get().getCoordinates());
  }

  @Test
  void testTagChange() {
    // node 2: creation and two tag changes, but no geometry changes

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 2,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();
    assertEquals(12, result.size());
    assertNotEquals(result.get(1).osmEntity().getTags(), result.get(0).osmEntity().getTags());
    assertEquals(result.get(2).osmEntity().getTags(), result.get(1).osmEntity().getTags());
    assertEquals(result.get(3).osmEntity().getTags(), result.get(2).osmEntity().getTags());
    assertEquals(result.get(4).osmEntity().getTags(), result.get(3).osmEntity().getTags());
    assertEquals(result.get(5).osmEntity().getTags(), result.get(4).osmEntity().getTags());
    assertEquals(result.get(6).osmEntity().getTags(), result.get(5).osmEntity().getTags());
    assertNotEquals(result.get(7).osmEntity().getTags(), result.get(6).osmEntity().getTags());
    assertEquals(result.get(8).osmEntity().getTags(), result.get(7).osmEntity().getTags());
    assertEquals(result.get(9).osmEntity().getTags(), result.get(8).osmEntity().getTags());
    assertEquals(result.get(10).osmEntity().getTags(), result.get(9).osmEntity().getTags());
    assertEquals(result.get(11).osmEntity().getTags(), result.get(10).osmEntity().getTags());
  }

  @Test
  void testVisibleChange() {
    // node 3: creation and 4 visible changes, but no geometry and no tag changes

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 3,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();
    assertEquals(5, result.size());
  }

  @Test
  void testMultipleChanges() {
    // node 4: creation and 5 changes:
    // tag and geometry,
    // visible = false,
    // visible = true and tag and geometry
    // geometry
    // tag

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 4,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();

    assertEquals(11, result.size());
    assertNotEquals(result.get(1).geometry().get().getCoordinates(),
        result.get(0).geometry().get().getCoordinates());
    assertArrayEquals(result.get(2).geometry().get().getCoordinates(),
        result.get(1).geometry().get().getCoordinates());
    assertNotEquals(result.get(3).geometry().get().getCoordinates(),
        result.get(2).geometry().get().getCoordinates());
    assertArrayEquals(result.get(5).geometry().get().getCoordinates(),
        result.get(3).geometry().get().getCoordinates());
    assertNotEquals(result.get(6).geometry().get().getCoordinates(),
        result.get(3).geometry().get().getCoordinates());
    assertArrayEquals(result.get(9).geometry().get().getCoordinates(),
        result.get(6).geometry().get().getCoordinates());
    assertNotEquals(result.get(1).osmEntity().getTags(),
        result.get(0).osmEntity().getTags());
    assertEquals(result.get(2).osmEntity().getTags(),
        result.get(1).osmEntity().getTags());
    assertNotEquals(result.get(3).osmEntity().getTags(),
        result.get(2).osmEntity().getTags());
    assertEquals(result.get(5).osmEntity().getTags(),
        result.get(4).osmEntity().getTags());
    assertNotEquals(result.get(9).osmEntity().getTags(),
        result.get(6).osmEntity().getTags());
  }

  @Test
  void testTagChangeTagFilterWithSuccess() {
    // node: creation then tag changes, but no geometry changes
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2006-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 5,
        osmEntity -> osmEntity.getTags().hasTagKey(osmXmlTestData.keys().get("shop")),
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();
    assertEquals(7, result.size());
  }

  @Test
  void testTagChangeTagFilterWithoutSuccess() {
    // node: creation then tag changes, but no geometry changes
    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        OSHDBBoundingBox.bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0),
        areaDecider,
        oshEntity -> oshEntity.getId() == 5,
        osmEntity -> osmEntity.getTags().hasTagKey(
            osmXmlTestData.keys().getOrDefault("amenity", -1)),
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();
    assertTrue(result.isEmpty());
  }

  @Test
  void testTagFilterAndPolygonIntersectingDataPartly() {
    // lon lat changes, so that node in v2 is outside bbox
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
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 6,
        osmEntity -> osmEntity.getTags().hasTagKey(osmXmlTestData.keys().get("shop")),
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).toList();
    assertEquals(1, result.size());
  }

  @Test
  void testCoordinatesRelativeToPolygon() throws IOException {
    //different cases of relative position between node coordinate(s) and cell bbox / query polygon
    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[4];
    coords[0] = new Coordinate(0.0, 0.0);
    coords[1] = new Coordinate(1.5, 0.0);
    coords[2] = new Coordinate(0.0, 1.5);
    coords[3] = new Coordinate(0.0, 0.0);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateByTimestampEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2007-01-01T00:00:00Z",
            "2008-01-01T00:00:00Z",
            "P1Y"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() >= 10 && oshEntity.getId() < 20,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        GridOSHFactory.getGridOSHNodes(osmXmlTestData, 6, (new XYGrid(6))
            .getId(1.0, 1.0)/* approx. 0, 0, 5.6, 5.6*/)
    ).toList();

    assertEquals(3, result.size());
    assertEquals(13, result.get(0).osmEntity().getId());
    assertEquals(13, result.get(1).osmEntity().getId());
    assertEquals(14, result.get(2).osmEntity().getId());
  }
}
