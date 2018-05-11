package org.heigit.bigspatialdata.oshdb.util.celliterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

public class IterateByTimestampsWaysTest {

  private GridOSHWays oshdbDataGridCell;
  private GridOSHNodes oshdbDataGridCellNodes;
  private final OSMXmlReader osmXmlTestData = new OSMXmlReader();
  TagInterpreter areaDecider;

  public IterateByTimestampsWaysTest() throws IOException {
    osmXmlTestData.add("./src/test/resources/different-timestamps/way.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    Map<Long, OSHNode> oshNodes = new TreeMap<>();
    for (Entry<Long, Collection<OSMNode>> entry : osmXmlTestData.nodes().asMap().entrySet()) {
      oshNodes.put(entry.getKey(), OSHNode.build(new ArrayList<>(entry.getValue())));
    }
    List<OSHWay> oshWays = new ArrayList<>();
    for (Entry<Long, Collection<OSMWay>> entry : osmXmlTestData.ways().asMap().entrySet()) {
      Collection<OSMWay> wayVersions = entry.getValue();
      oshWays.add(OSHWay.build(new ArrayList<>(wayVersions),
          wayVersions.stream().flatMap(osmWay ->
              Arrays.stream(osmWay.getRefs()).map(ref -> oshNodes.get(ref.getId()))
          ).collect(Collectors.toSet())
      ));
    }
    oshdbDataGridCell = GridOSHWays.compact(-1, -1, 0, 0, 0, 0, oshWays);
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
        new OSHDBBoundingBox(-180, -90, 180, 90),
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
    assertEquals(31, result.get(0).osmEntity.getChangeset());
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
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 101,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(10, result.size());

    assertEquals(34, result.get(0).osmEntity.getChangeset());
    assertEquals(35, result.get(8).osmEntity.getChangeset());

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
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 102,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(10, result.size());

    assertEquals(36, result.get(0).osmEntity.getChangeset());
    assertEquals(38, result.get(9).osmEntity.getChangeset());
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
        new OSHDBBoundingBox(-180,-90, 180, 90),
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
        new OSHDBBoundingBox(-180,-90, 180, 90),
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

    assertEquals(42, result.get(0).osmEntity.getChangeset());
    assertEquals(43, result.get(7).osmEntity.getChangeset());

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
        new OSHDBBoundingBox(-180,-90, 180, 90),
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
        new OSHDBBoundingBox(-180,-90, 180, 90),
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
        new OSHDBBoundingBox(-180,-90, 180, 90),
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
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 108,
        osmEntity -> true,
        false
    )).iterateByTimestamps(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(9, result.size());
  }
}
