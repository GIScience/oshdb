package org.heigit.bigspatialdata.oshdb.util.celliterator.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHWayImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.xmlreader.OSMXmlReader;


/**
 * Helper class to get GridOSH's (Holds the basic information, every OSM-Object has at a
 * specific level) out of osm-xml file
 */

public class GridOSHFactory {

  public static GridOSHNodes getGridOSHNodes(OSMXmlReader osmXmlReader) throws IOException {
    return getGridOSHNodes(osmXmlReader, -1, -1);
  }

  public static GridOSHWays getGridOSHWays(OSMXmlReader osmXmlReader) throws IOException {
    return getGridOSHWays(osmXmlReader, -1, -1);
  }

  public static GridOSHRelations getGridOSHRelations(OSMXmlReader osmXmlReader) throws IOException {
    return getGridOSHRelations(osmXmlReader, -1, -1);
  }

  public static GridOSHNodes getGridOSHNodes(OSMXmlReader osmXmlReader, int cellZoom, long cellId) throws IOException {
    GridOSHNodes oshdbDataGridCellNodes;
    List<OSHNode> oshNodes = new ArrayList<>();
    for (Entry<Long, Collection<OSMNode>> entry : osmXmlReader.nodes().asMap().entrySet()) {
      oshNodes.add(OSHNodeImpl.build(new ArrayList<>(entry.getValue())));
    }
    oshdbDataGridCellNodes = GridOSHNodes.rebase(cellId, cellZoom, 0, 0, 0, 0,
        oshNodes
    );
    return oshdbDataGridCellNodes;
  }

  public static GridOSHWays getGridOSHWays(OSMXmlReader osmXmlReader, int cellZoom, long cellId) throws IOException {
    GridOSHWays oshdbDataGridCellWays;
    Map<Long, OSHNode> oshNodes = getOSHNodes(osmXmlReader);
    List<OSHWay> oshWays = new ArrayList<>();
    for (Entry<Long, Collection<OSMWay>> entry : osmXmlReader.ways().asMap().entrySet()) {
      Collection<OSMWay> wayVersions = entry.getValue();
      oshWays.add(OSHWayImpl.build(new ArrayList<>(wayVersions),
          wayVersions.stream().flatMap(osmWay ->
              Arrays.stream(osmWay.getRefs()).map(ref -> oshNodes.get(ref.getId()))
          ).collect(Collectors.toSet())
      ));
    }
    oshdbDataGridCellWays = GridOSHWays.compact(-1, -1, 0, 0, 0, 0, oshWays);
    return oshdbDataGridCellWays;
  }

  public static GridOSHRelations getGridOSHRelations(OSMXmlReader osmXmlReader, int cellZoom, long cellId) throws IOException {
    Map<Long, OSHNode> oshNodes = getOSHNodes(osmXmlReader);
    Map<Long, OSHWay> oshWays = getOSHWays(osmXmlReader);

    GridOSHRelations oshdbDataGridCellRelations;
    List<OSHRelation> oshRelations = new ArrayList<>();
    for (Entry<Long, Collection<OSMRelation>> entry : osmXmlReader.relations().asMap().entrySet()) {
      Collection<OSMRelation> relationVersions = entry.getValue();
      oshRelations.add(OSHRelationImpl.build(new ArrayList<>(relationVersions),
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
    oshdbDataGridCellRelations = GridOSHRelations.compact(0, 0, 0, 0, 0, 0, oshRelations);
    return oshdbDataGridCellRelations;
  }

  private static Map<Long, OSHNode> getOSHNodes(OSMXmlReader osmXmlReader) throws IOException {
    Map<Long, OSHNode> oshNodes = new TreeMap<>();
    for (Entry<Long, Collection<OSMNode>> entry : osmXmlReader.nodes().asMap().entrySet()) {
      oshNodes.put(entry.getKey(), OSHNodeImpl.build(new ArrayList<>(entry.getValue())));
    }
    return oshNodes;
  }

  private static Map<Long, OSHWay> getOSHWays(OSMXmlReader osmXmlReader)throws IOException {
    Map<Long, OSHNode> oshNodes = getOSHNodes(osmXmlReader);
    Map<Long, OSHWay> oshWays = new TreeMap<>();
    for (Entry<Long, Collection<OSMWay>> entry : osmXmlReader.ways().asMap().entrySet()) {
      Collection<OSMWay> wayVersions = entry.getValue();
      oshWays.put(entry.getKey(), OSHWayImpl.build(new ArrayList<>(wayVersions),
          wayVersions.stream().flatMap(osmWay ->
              Arrays.stream(osmWay.getRefs()).map(ref -> oshNodes.get(ref.getId()))
          ).collect(Collectors.toSet())
      ));
    }
    return oshWays;
  }
}
