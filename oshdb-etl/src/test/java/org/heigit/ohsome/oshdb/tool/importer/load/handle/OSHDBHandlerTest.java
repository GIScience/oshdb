package org.heigit.ohsome.oshdb.tool.importer.load.handle;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransfomRelation;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHNode;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHWay;
import org.heigit.ohsome.oshdb.util.bytearray.ByteArrayOutputWrapper;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


public class OSHDBHandlerTest {
  private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baAux = new ByteArrayOutputWrapper(1024);

  @Test
  public void testNodeGrid() throws IOException {
    var nodes = new ArrayList<OSMNode>();
    nodes.add(new OSMNode(1, 1, 1000, 100, 23, new int[0], 0, 0));
    var handler = new Adapter(null, null) {
      GridOSHNodes grid;
      @Override
      public void handleNodeGrid(GridOSHNodes grid) {
        this.grid = grid;
      }
    };
    handler.handleNodeGrid(0, List.of(
        TransformOSHNode.build(baData, baRecord, baAux, nodes,
        0, 0L, 0, 0)));
    var grid = handler.grid;
    assertEquals(1, Iterables.size(grid.getEntities()));
  }

  @Test
  public void testWayGrid() throws IOException {
    var nodes = new ArrayList<OSMNode>();
    nodes.add(new OSMNode(1, 1, 1000, 100, 23, new int[0], 0, 0));
    TransformOSHNode.build(baData, baRecord, baAux, nodes, 0, 0L, 0, 0);
    final byte[] record = new byte[baRecord.length()];
    System.arraycopy(baRecord.array(), 0, record, 0, record.length);
    var tnodes = List.of(TransformOSHNode.instance(record, 0, record.length));
    LongSortedSet nodeIds = new LongAVLTreeSet();
    nodeIds.add(1);
    var ways = new ArrayList<OSMWay>();
    ways.add(new OSMWay(1, 1, 1000, 100, 23, new int[0],
        new OSMMember[] {new OSMMember(1, OSMType.NODE, 0)}));
    var handler = new Adapter(new Roaring64NavigableMap(), new Roaring64NavigableMap()) {
      GridOSHWays grid;
      @Override
      public void handleWayGrid(GridOSHWays grid) {
        this.grid = grid;
      }
    };
    handler.handleWayGrid(0, List.of(
        TransformOSHWay.build(baData, baRecord, baAux, ways, nodeIds,
        0, 0L, 0, 0)), tnodes);
    var grid = handler.grid;
    assertEquals(1, Iterables.size(grid.getEntities()));
  }

  @Test
  public void testRelGrid() throws IOException {
    var nodes = new ArrayList<OSMNode>();
    nodes.add(new OSMNode(1, 1, 1000, 100, 23, new int[0], 0, 0));
    TransformOSHNode.build(baData, baRecord, baAux, nodes, 0, 0L, 0, 0);
    final byte[] record = new byte[baRecord.length()];
    System.arraycopy(baRecord.array(), 0, record, 0, record.length);
    var tnodes = List.of(TransformOSHNode.instance(record, 0, record.length));
    LongSortedSet nodeIds = new LongAVLTreeSet();
    nodeIds.add(1);
    var tways = List.<TransformOSHWay>of();
    LongSortedSet wayIds = new LongAVLTreeSet();
    var relations = new ArrayList<OSMRelation>();
    relations.add(new OSMRelation(1, 1, 1000, 100, 23, new int[0],
        new OSMMember[] {new OSMMember(1, OSMType.NODE, 0)}));
    var handler = new Adapter(null, null) {
      GridOSHRelations grid;
      @Override
      public void handleRelationsGrid(GridOSHRelations grid) {
        this.grid = grid;
      }
    };
    handler.handleRelationGrid(0, List.of(
        TransfomRelation.build(baData, baRecord, baAux, relations, nodeIds, wayIds, 0L, 0L, 0, 0)),
        tnodes, tways);
    var grid = handler.grid;
    assertEquals(1, Iterables.size(grid.getEntities()));
  }

  private static class Adapter extends OSHDBHandler {

    protected Adapter(Roaring64NavigableMap bitmapNodeRelation,
        Roaring64NavigableMap bitmapWayRelation) {
      super(bitmapNodeRelation, bitmapWayRelation);
    }

    @Override
    public void handleNodeGrid(GridOSHNodes grid) {}

    @Override
    public void handleWayGrid(GridOSHWays grid) {}

    @Override
    public void handleRelationsGrid(GridOSHRelations grid) {}
  }
}
