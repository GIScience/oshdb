package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransfomRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.SortedLong2LongMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.RelationMember;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class TransformerRelation extends Transformer {
  private final ByteArrayOutputWrapper wrapperData = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper wrapperRecord = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper wrapperNodeData = new ByteArrayOutputWrapper(1024);
 
  final SortedLong2LongMap nodeToCell;
  final SortedLong2LongMap wayToCell;

  public TransformerRelation(long maxMemory,int maxZoom, Path workDirectory, TagToIdMapper tagToIdMapper, RoleToIdMapper role2Id,
      SortedLong2LongMap nodeToCell, SortedLong2LongMap wayToCell, int workerId) throws IOException {
    super(maxMemory,maxZoom, workDirectory, tagToIdMapper,role2Id,workerId);
    this.nodeToCell = nodeToCell;
    this.wayToCell = wayToCell;
    
  }

  public OSMType type() {
    return OSMType.RELATION;
  }

  private Roaring64NavigableMap bitmapRefNode = new Roaring64NavigableMap();
  private Roaring64NavigableMap bitmapRefWay = new Roaring64NavigableMap();
  
  
  public void transform(long id, List<Entity> versions) {
    List<OSMRelation> entities = new ArrayList<>(versions.size());
    LongSortedSet nodeIds = new LongAVLTreeSet();
    LongSortedSet wayIds = new LongAVLTreeSet();
    for (Entity version : versions) {
      final Relation entity = (Relation) version;
      final OSMRelation osm = getOSM(entity);
      entities.add(osm);

      for (OSMMember member : osm.getMembers()) {
        final OSMType type = member.getType();
        if (type == OSMType.NODE){
          nodeIds.add(member.getId());
          bitmapRefNode.add(member.getId());
        }
        else if (type == OSMType.WAY){
          wayIds.add(member.getId());
          bitmapRefWay.add(member.getId());
        }
      }
    }

    LongSet cellIds;
    if (!nodeIds.isEmpty()) {
      cellIds = nodeToCell.get(nodeIds);
      cellIds.addAll(wayToCell.get(wayIds));
    } else {
      cellIds = wayToCell.get(wayIds);
    }

    final long cellId = findBestFittingCellId(cellIds);
    try {
      final LongFunction<byte[]> toByteArray = baseId -> {
        try {
          TransfomRelation osh = TransfomRelation.build(wrapperData, wrapperRecord, wrapperNodeData,entities, nodeIds, wayIds, baseId, 0, 0, 0);

          final byte[] record = new byte[wrapperRecord.length()];
          System.arraycopy(wrapperRecord.array(), 0, record, 0, record.length);

          return record;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      };

      store(cellId,id, toByteArray,nodeIds,wayIds);
      addIdToCell(id, cellId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void complete() {
    super.complete();
    
    bitmapRefNode.runOptimize();
    try(FileOutputStream fileOut = new FileOutputStream(workDirectory.resolve("transform_nodeWithRelation.bitmap").toFile());
        ObjectOutputStream out = new ObjectOutputStream(fileOut)){
      bitmapRefNode.writeExternal(out);
    } catch (IOException e) {
      e.printStackTrace();
    }
    bitmapRefWay.runOptimize();
    try(FileOutputStream fileOut = new FileOutputStream(workDirectory.resolve("transform_wayWithRelation.bitmap").toFile());
        ObjectOutputStream out = new ObjectOutputStream(fileOut)){
      bitmapRefWay.writeExternal(out);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  
  private OSMRelation getOSM(Relation entity) {
    return new OSMRelation(entity.getId() //
        , modifiedVersion(entity) //
        , new OSHDBTimestamp(entity.getTimestamp()) //
        , entity.getChangeset() //
        , entity.getUserId() //
        , getKeyValue(entity.getTags()) //
        , convertToOSMMembers(entity.getMembers()));
  }

  private OSMMember[] convertToOSMMembers(RelationMember[] members) {
    OSMMember[] ret = new OSMMember[members.length];
    int i = 0;
    for (RelationMember member : members) {
      ret[i++] = new OSMMember(member.memId, OSMType.fromInt(member.type), getRole(member.role));
    }
    return ret;
  }
}
