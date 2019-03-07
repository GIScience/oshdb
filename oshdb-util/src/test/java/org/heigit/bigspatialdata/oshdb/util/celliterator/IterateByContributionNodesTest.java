package org.heigit.bigspatialdata.oshdb.util.celliterator;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.bigspatialdata.oshdb.util.celliterator.helpers.GridOSHFactory;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class IterateByContributionNodesTest {
  private GridOSHNodes oshdbDataGridCell;
  private final OSMXmlReader osmXmlTestData = new OSMXmlReader();
  TagInterpreter areaDecider;

  public IterateByContributionNodesTest() throws IOException {
    osmXmlTestData.add("./src/test/resources/different-timestamps/node.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    oshdbDataGridCell = GridOSHFactory.getGridOSHNodes(osmXmlTestData);
  }

  @Test
  public void testGeometryChange() {
    // node 1: creation and two geometry changes, but no tag changes

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 1,
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
    assertEquals(1, result.get(0).changeset);
    assertEquals(null, result.get(0).previousGeometry.get());
    Geometry geom = result.get(0).geometry.get();
    assertTrue(geom instanceof Point);
    assertEquals(result.get(0).geometry.get(), result.get(1).previousGeometry.get());
    assertNotEquals(result.get(1).geometry.get(), result.get(1).previousGeometry.get());
    assertEquals(result.get(1).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
  }

  @Test
  public void testTagChange() {
    // node 2: creation and two tag changes, but no geometry changes

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 2,
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
    assertEquals(3, result.get(0).changeset);
    assertNotEquals(result.get(1).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertNotEquals(result.get(2).osmEntity.getRawTags(), result.get(1).osmEntity.getRawTags());
  }

  @Test
  public void testVisibleChange() {
    // node 3: creation and 4 visible changes, but no geometry and no tag changes

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 3,
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
        EnumSet.of(ContributionType.DELETION),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(2).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(3).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(4).activities.get()
    );
    assertEquals(6, result.get(0).changeset);
  }

  @Test
  public void testMultipleChanges() {
    // node 4: creation and 5 changes:
    // tag and geometry,
    // visible = false,
    // visible = true and tag and geometry
    // geometry
    // tag

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 4,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(6, result.size());
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(0).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE,ContributionType.GEOMETRY_CHANGE),
        result.get(1).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(2).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.CREATION),
        result.get(3).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.GEOMETRY_CHANGE),
        result.get(4).activities.get()
    );
    assertEquals(
        EnumSet.of(ContributionType.TAG_CHANGE),
        result.get(5).activities.get()
    );
    assertEquals(11, result.get(0).changeset);
    assertNotEquals(result.get(1).osmEntity.getRawTags(), result.get(0).osmEntity.getRawTags());
    assertNotEquals(result.get(3).osmEntity.getRawTags(), result.get(1).osmEntity.getRawTags());
    assertEquals(result.get(4).osmEntity.getRawTags(), result.get(3).osmEntity.getRawTags());
    assertNotEquals(result.get(5).osmEntity.getRawTags(), result.get(4).osmEntity.getRawTags());
  }

  @Test
  public void testBboxMinAndMaxNotCorrect() {
    // node 1: creation and two geometry changes, but no tag changes
    // OSHDBBoundingBox: MinLon and MinLat as well as MaxLon and MaxLat incorrect
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(8, 9, 49, 50),
        areaDecider,
        oshEntity -> oshEntity.getId() == 1,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBboxMinExactlyAtDataMinMaxExcluded() {
    // node 1: creation and two geometry changes, but no tag changes
    // OSHDBBoundingBox: MinLon and MinLat like Version 1, MaxLon and MaxLat incorrect
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(1.42, 1.22, 1.3, 1.1),
        areaDecider,
        oshEntity -> oshEntity.getId() == 1,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBboxMaxExactlyAtDataMaxMinExcluded() {
    // node 1: creation and two geometry changes, but no tag changes
    // OSHDBBoundingBox: MinLon and MinLat incorrect, MaxLon and MaxLat like Version 3
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(3.2, 3.3, 1.425, 1.23),
        areaDecider,
        oshEntity -> oshEntity.getId() == 1,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testBboxMinMaxExactlyAtDataMinMax() {
    // node 1: creation and two geometry changes, but no tag changes
    // OSHDBBoundingBox: MinLon and MinLat like Version 1, MaxLon and MaxLat like Version 3
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(1.42, 1.22, 1.425, 1.23),
        areaDecider,
        oshEntity -> oshEntity.getId() == 1,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(3, result.size());
  }

  @Test
  public void testTagChangeTagFilterWithSuccess() {
    // node: creation then tag changes, but no geometry changes
    // check if results are correct if we filter for a special tag
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 5,
        osmEntity -> osmEntity.hasTagKey(osmXmlTestData.keys().get("shop")),
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(4, result.size());
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
    assertEquals(
        EnumSet.of(ContributionType.DELETION),
        result.get(3).activities.get()
    );
  }

  @Test
  public void testTagChangeTagFilterDisused() {
    // check if results are correct if we filter for a special tag
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2007-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 7,
        osmEntity -> osmEntity.hasTagKey(osmXmlTestData.keys().get("disused:shop")),
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
  }

  @Test
  public void testMoreComplicatedFilter() {
    // check if results are correct if we filter for a special tag
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2007-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(0,0, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 8,
        osmEntity -> osmEntity.hasTagKey(osmXmlTestData.keys().get("shop")),
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());

    assertEquals(4, result.size());
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
  }

  @Test
  public void testTagChangeTagFilterWithoutSuccess() {
    // check if results are correct if we filter for a special tag
    // tag not in data
    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        new OSHDBBoundingBox(-180,-90, 180, 90),
        areaDecider,
        oshEntity -> oshEntity.getId() == 5,
        osmEntity -> osmEntity.hasTagKey(osmXmlTestData.keys().getOrDefault("amenity", -1)),
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testPolygonIntersectingDataPartly() {
    // lon lat changes, so that node in v2 is outside bbox
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords=new Coordinate[5];
    coords[0]=new Coordinate(10.8,10.3);
    coords[1]=new Coordinate(10.8 ,22.7);
    coords[2]=new Coordinate(22.7,22.7);
    coords[3]=new Coordinate(22.7,10.3);
    coords[4]=new Coordinate(10.8,10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 6,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertEquals(2,result.size());
  }

  @Test
  public void testTagFilterAndPolygonIntersectingDataPartly() {
    // lon lat changes, so that node in v2 is outside bbox
    GeometryFactory geometryFactory = new GeometryFactory(); // create clipping polygon for area of interest
    Coordinate[] coords=new Coordinate[5];
    coords[0]=new Coordinate(10.8,10.3);
    coords[1]=new Coordinate(10.8 ,22.7);
    coords[2]=new Coordinate(22.7,22.7);
    coords[3]=new Coordinate(22.7,10.3);
    coords[4]=new Coordinate(10.8,10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,// clipping polygon
        areaDecider,
        oshEntity -> oshEntity.getId() == 6,
        osmEntity -> osmEntity.hasTagKey(osmXmlTestData.keys().get("shop")),// filter entity for tag = shop
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    // result size =2 becuase if tag filtered for disappears it's a deletion
    assertEquals(2,result.size());// one version with tag shop
  }

  @Test
  public void testCoordinatesRelativeToPolygon() throws IOException {
    //different cases of relative position between node coordinate(s) and cell bbox / query polygon
    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords=new Coordinate[4];
    coords[0]=new Coordinate(0.0,0.0);
    coords[1]=new Coordinate(1.5,0.0);
    coords[2]=new Coordinate(0.0,1.5);
    coords[3]=new Coordinate(0.0,0.0);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> result = (new CellIterator(
        new OSHDBTimestamps(
            "2007-01-01T00:00:00Z",
            "2009-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() >= 10 && oshEntity.getId() < 20,
        osmEntity -> true,
        false
    )).iterateByContribution(
        GridOSHFactory.getGridOSHNodes(osmXmlTestData, 6, (new XYGrid(6)).getId(1.0, 1.0)/* approx. 0,0,5.6,5.6*/)
    ).collect(Collectors.toList());

    assertEquals(2, result.size());
    assertTrue(result.get(0).osmEntity.getId() == 13);
    assertTrue(result.get(1).osmEntity.getId() == 14);
  }
}
