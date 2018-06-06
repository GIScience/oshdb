package org.heigit.bigspatialdata.oshdb.util.celliterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateAllEntry;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.IterateByTimestampEntry;
import org.heigit.bigspatialdata.oshdb.util.geometry.helpers.OSMXmlReaderTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.test.OSMXmlReader;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

public class IterateByContributionNotOsmTypeSpecific {

  private final OSMXmlReader osmXmlTestData = new OSMXmlReader();
  TagInterpreter areaDecider;
  private final List<OSHRelation> oshRelations = new ArrayList<>();
  private final double DELTA = 1E-6;

  public IterateByContributionNotOsmTypeSpecific() throws IOException {
    osmXmlTestData.add("./src/test/resources/different-timestamps/polygon.osm");
    areaDecider = new OSMXmlReaderTagInterpreter(osmXmlTestData);
    Map<Long, OSHNode> oshNodes = new TreeMap<>();
    for (Entry<Long, Collection<OSMNode>> entry : osmXmlTestData.nodes().asMap().entrySet()) {
      oshNodes.put(entry.getKey(), OSHNode.build(new ArrayList<>(entry.getValue())));
    }
    Map<Long, OSHWay> oshWays = new TreeMap<>();
    for (Entry<Long, Collection<OSMWay>> entry : osmXmlTestData.ways().asMap().entrySet()) {
      Collection<OSMWay> wayVersions = entry.getValue();
      oshWays.put(entry.getKey(), OSHWay.build(new ArrayList<>(wayVersions),
          wayVersions.stream().flatMap(osmWay ->
              Arrays.stream(osmWay.getRefs()).map(ref -> oshNodes.get(ref.getId()))
          ).collect(Collectors.toSet())
      ));
    }

    for (Entry<Long, Collection<OSMRelation>> entry : osmXmlTestData.relations().asMap().entrySet()) {
      Collection<OSMRelation> relationVersions = entry.getValue();
      oshRelations.add(OSHRelation.build(new ArrayList<>(relationVersions),
          relationVersions.stream().flatMap(osmRelation ->
              Arrays.stream(osmRelation.getMembers())
                  .filter(member -> member.getType() == OSMType.NODE)
                  .map(member -> oshNodes.get(member.getId()))
          ).collect(Collectors.toSet()),
          relationVersions.stream().flatMap(osmRelation ->
              Arrays.stream(osmRelation.getMembers())
                  .filter(member -> member.getType() == OSMType.WAY)
                  .map(member -> oshWays.get(member.getId()))
          ).collect(Collectors.toSet())
      ));
    }
  }

  @Test
  public void testCellOutsidePolygon() throws IOException {
    GridOSHRelations oshdbDataGridCell = GridOSHRelations.compact(69120, 12, 0, 0, 0, 0, Collections
        .emptyList());

    GeometryFactory geometryFactory = new GeometryFactory();
    Coordinate[] coords=new Coordinate[5];
    coords[0]=new Coordinate(10.8,10.3);
    coords[1]=new Coordinate(10.8 ,12.7);
    coords[2]=new Coordinate(12.7,12.7);
    coords[3]=new Coordinate(12.7,10.3);
    coords[4]=new Coordinate(10.8,10.3);
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
    GridOSHRelations oshdbDataGridCell = GridOSHRelations.compact(0, 0, 0, 0, 0, 0, oshRelations);

    GeometryFactory geometryFactory = new GeometryFactory();

    Coordinate[] coords=new Coordinate[4];
    coords[0]=new Coordinate(10.8,10.3);
    coords[1]=new Coordinate(12.7,12.7);
    coords[2]=new Coordinate(12.7,10.3);
    coords[3]=new Coordinate(10.8,10.3);
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
    GridOSHRelations oshdbDataGridCell = GridOSHRelations.compact(69120, 12, 0, 0, 0, 0, oshRelations);

    GeometryFactory geometryFactory = new GeometryFactory();

    Coordinate[] coords=new Coordinate[5];
    coords[0]=new Coordinate(-180,-90);
    coords[1]=new Coordinate(180 ,-90);
    coords[2]=new Coordinate(180,90);
    coords[3]=new Coordinate(-180,90);
    coords[4]=new Coordinate(-180,-90);
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
    assertTrue(!resultPoly.isEmpty());
  }




}