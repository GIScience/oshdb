package org.heigit.ohsome.oshdb.tool.importer.load.handle;

import it.unimi.dsi.fastutil.longs.Long2ObjectAVLTreeMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.index.XYGrid;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransfomRelation;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHNode;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHWay;
import org.heigit.ohsome.oshdb.tool.importer.util.ZGrid;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public abstract class OSHDBHandler extends LoaderHandler {

  protected final Roaring64NavigableMap bitmapNodeRelation;
  protected final Roaring64NavigableMap bitmapWayRelation;

  protected OSHDBHandler(Roaring64NavigableMap bitmapNodeRelation,
      Roaring64NavigableMap bitmapWayRelation) {
    this.bitmapNodeRelation = bitmapNodeRelation;
    this.bitmapWayRelation = bitmapWayRelation;
  }

  public abstract void handleNodeGrid(GridOSHNodes grid);

  @Override
  public void handleNodeGrid(long zid, Collection<TransformOSHNode> nodes) {
    if (zid < 0) {
      return;
    }
    List<OSHNode> entries = nodes.stream().map(osh2 -> {
      List<OSMNode> versions = osh2.stream().collect(Collectors.toList());
      return OSHNodeImpl.build(versions);
    }).collect(Collectors.toList());

    if (!entries.isEmpty()) {
      entries.sort((a, b) -> Long.compare(a.getId(), b.getId()));
      var grid = grid(zid, (xyId, zoom, lon, lat) ->
          GridOSHNodes.rebase(xyId, zoom, entries.get(0).getId(), 0, lon, lat, entries));
      handleNodeGrid(grid);
    }
  }

  protected Long2ObjectAVLTreeMap<OSHWay> waysForRelation = new Long2ObjectAVLTreeMap<>();

  public abstract void handleWayGrid(GridOSHWays grid);

  @Override
  public void handleWayGrid(long zid, Collection<TransformOSHWay> ways,
      Collection<TransformOSHNode> nodes) {
    if (zid < 0) {
      return;
    }
    var idOshMap = mapNodes(nodes);

    List<OSHWay> entities = ways.stream().map(tosh -> {
      List<OSMWay> versions = tosh.stream().collect(Collectors.toList());

      List<OSHNode> nodesForThisWay = new ArrayList<>(tosh.getNodeIds().length);
      for (long id : tosh.getNodeIds()) {
        if (idOshMap.get(id) == null) {
          continue;
        }
        nodesForThisWay.add(idOshMap.get(id));
      }

      var osh = OSHWayImpl.build(versions, nodesForThisWay);
      if (bitmapWayRelation.contains(osh.getId())) {
        waysForRelation.put(osh.getId(), osh);
      }
      return osh;
    }).collect(Collectors.toList());

    if (!entities.isEmpty()) {
      entities.sort((a, b) -> Long.compare(a.getId(), b.getId()));
      var grid = grid(zid, (xyId, zoom, lon, lat) ->
          GridOSHWays.compact(xyId, zoom, entities.get(0).getId(), 0, lon, lat, entities));
      handleWayGrid(grid);
    }
  }

  private <T> T grid(long zid, GridInstance<T> newInstance) {
    OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zid);
    int longitude = bbox.getMinLon() + (bbox.getMaxLon() - bbox.getMinLon()) / 2;
    int latitude = bbox.getMinLat() + (bbox.getMaxLat() - bbox.getMinLat()) / 2;
    int zoom = ZGrid.getZoom(zid);
    var xyGrid = new XYGrid(zoom);
    long xyId = xyGrid.getId(longitude, latitude);
    return newInstance.newGrid(xyId, zoom, longitude, latitude);
  }

  @FunctionalInterface
  private interface GridInstance<T> {
    T newGrid(long xyId, int zoom, int longitude, int latitude);
  }

  private Map<Long, OSHNode> mapNodes(Collection<TransformOSHNode> nodes) {
    Map<Long, OSHNode> idOshMap = new HashMap<>(nodes.size());
    nodes.forEach(osh2 -> {
      Long id = osh2.getId();
      List<OSMNode> versions = osh2.stream().collect(Collectors.toList());
      OSHNode osh = OSHNodeImpl.build(versions);
      idOshMap.put(id, osh);
    });
    return idOshMap;
  }

  public abstract void handleRelationsGrid(GridOSHRelations grid);

  @Override
  public void handleRelationGrid(long zid, Collection<TransfomRelation> relations,
      Collection<TransformOSHNode> nodes, Collection<TransformOSHWay> ways) {
    if (zid < 0) {
      return;
    }

    Map<Long, OSHNode> idOshMap = mapNodes(nodes);
    List<OSHRelation> entities = relations.stream().map(osh2 -> {
      List<OSMRelation> versions = osh2.stream().collect(Collectors.toList());

      List<OSHNode> nodesForThisRelation = new ArrayList<>(osh2.getNodeIds().length);
      for (long id : osh2.getNodeIds()) {
        OSHNode node = idOshMap.get(id);
        if (node != null) {
          nodesForThisRelation.add(node);
        }
      }

      List<OSHWay> waysForThisRelation = new ArrayList<>(osh2.getWayIds().length);
      for (long id : osh2.getWayIds()) {
        OSHWay way = waysForRelation.get(id);
        if (way != null) {
          waysForThisRelation.add(way);
        }
      }

      return OSHRelationImpl.build(versions, nodesForThisRelation, waysForThisRelation);
    }).collect(Collectors.toList());

    if (!entities.isEmpty()) {
      entities.sort((a, b) -> Long.compare(a.getId(), b.getId()));
      var grid = grid(zid, (xyId, zoom, lon, lat) ->
          GridOSHRelations.compact(xyId, zoom, entities.get(0).getId(), 0, lon, lat, entities));
      handleRelationsGrid(grid);
    }
  }
}
