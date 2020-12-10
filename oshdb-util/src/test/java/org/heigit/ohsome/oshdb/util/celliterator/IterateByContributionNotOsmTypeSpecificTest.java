package org.heigit.ohsome.oshdb.util.celliterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.ohsome.oshdb.util.celliterator.helpers.GridOSHFactory;
import org.heigit.ohsome.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.heigit.ohsome.oshdb.util.xmlreader.OSMXmlReader;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

public class IterateByContributionNotOsmTypeSpecificTest {

  TagInterpreter areaDecider;
  private final List<OSHRelation> oshRelations;

  /**
   * Initialize test framework by loading osm XML file and initializing {@link TagInterpreter} and
   * a list of {@link OSHRelation OSHRelations}.
   */
  public IterateByContributionNotOsmTypeSpecificTest() throws IOException {
    OSMXmlReader osmXmlTestData = new OSMXmlReader();
    osmXmlTestData.add("./src/test/resources/different-timestamps/polygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    GridOSHRelations oshdbDataGridCell = GridOSHFactory.getGridOSHRelations(osmXmlTestData);
    oshRelations = Lists.newArrayList((Iterable<OSHRelation>) oshdbDataGridCell.getEntities());
  }

  @Test
  public void testCellOutsidePolygon() throws IOException {
    final GridOSHRelations oshdbDataGridCell = GridOSHRelations.compact(69120, 12, 0, 0, 0, 0,
        Collections.emptyList());

    final GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(10.8, 12.7);
    coords[2] = new Coordinate(12.7, 12.7);
    coords[3] = new Coordinate(12.7, 10.3);
    coords[4] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> resultPoly = (new CellIterator(
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
    assertTrue(resultPoly.isEmpty());
  }

  @Test
  public void testCellCoveringPolygon() throws IOException {
    final GridOSHRelations oshdbDataGridCell = GridOSHRelations.compact(0, 0, 0, 0, 0, 0,
        oshRelations);

    final GeometryFactory geometryFactory = new GeometryFactory();

    Coordinate[] coords = new Coordinate[4];
    coords[0] = new Coordinate(10.8, 10.3);
    coords[1] = new Coordinate(12.7, 12.7);
    coords[2] = new Coordinate(12.7, 10.3);
    coords[3] = new Coordinate(10.8, 10.3);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> resultPoly = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> oshEntity.getId() == 80,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertTrue(resultPoly.isEmpty());
  }

  @Test
  public void testCellFullyInsidePolygon() throws IOException {
    final GridOSHRelations oshdbDataGridCell = GridOSHRelations.compact(69120, 12, 0, 0, 0, 0,
        oshRelations);

    final GeometryFactory geometryFactory = new GeometryFactory();

    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(-180, -90);
    coords[1] = new Coordinate(180, -90);
    coords[2] = new Coordinate(180, 90);
    coords[3] = new Coordinate(-180, 90);
    coords[4] = new Coordinate(-180, -90);
    Polygon polygonFromCoordinates = geometryFactory.createPolygon(coords);

    List<IterateAllEntry> resultPoly = (new CellIterator(
        new OSHDBTimestamps(
            "2000-01-01T00:00:00Z",
            "2018-01-01T00:00:00Z"
        ).get(),
        polygonFromCoordinates,
        areaDecider,
        oshEntity -> true,
        osmEntity -> true,
        false
    )).iterateByContribution(
        oshdbDataGridCell
    ).collect(Collectors.toList());
    assertFalse(resultPoly.isEmpty());
  }




}
