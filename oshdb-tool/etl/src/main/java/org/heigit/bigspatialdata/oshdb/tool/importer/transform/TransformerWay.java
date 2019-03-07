package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.osh.TransformOSHWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.SortedLong2LongMap;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Way;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class TransformerWay extends Transformer {
  private final ByteArrayOutputWrapper baData = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper baRecord = new ByteArrayOutputWrapper(1024);
  private final ByteArrayOutputWrapper wrapperNodeData = new ByteArrayOutputWrapper(1024);
  
  final SortedLong2LongMap nodeToCell;

  
  public TransformerWay(long maxMemory,int maxZoom,Path workDirectory,TagToIdMapper tagToIdMapper,SortedLong2LongMap nodeToCell, int workerId) throws IOException {
    super(maxMemory,maxZoom, workDirectory,tagToIdMapper,workerId);
    this.nodeToCell = nodeToCell;
  }
  public OSMType type(){
    return OSMType.WAY;
  }
  
  private final long[] lastDataSize = new long[2];
  
  public void transform(long id, List<Entity> versions) {    
    List<OSMWay> ways = new ArrayList<>(versions.size());
    LongSortedSet nodeIds = new LongAVLTreeSet();
    for (Entity version : versions) {
      Way way = (Way) version;
      ways.add(getOSM(way));
      for (long ref : way.refs) {
        nodeIds.add(ref);
      }
    }
    
    
    LongSet cellIds = nodeToCell.get(nodeIds);
        
    final long cellId =  findBestFittingCellId(cellIds);
        
    try {
      final LongFunction<byte[]> toByteArray = baseId -> {
        try {
      TransformOSHWay osh = TransformOSHWay.build(baData,baRecord,wrapperNodeData,ways,nodeIds,baseId,0,0,0);
         
      final byte[] record = new byte[baRecord.length()];
      System.arraycopy(baRecord.array(), 0, record, 0, record.length);

      return record;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };
        
     store(cellId,id, toByteArray, nodeIds);
     addIdToCell(id, cellId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
     
  private OSMWay getOSM(Way entity) {
    return new OSMWay(entity.getId() //
        , modifiedVersion(entity) //
        , new OSHDBTimestamp(entity.getTimestamp()) //
        , entity.getChangeset() //
        , entity.getUserId() //
        , getKeyValue(entity.getTags()) //
        , convertNodeIdsToOSMMembers(entity.getRefs()));
  }

  private OSMMember[] convertNodeIdsToOSMMembers(long[] refs) {
    OSMMember[] ret = new OSMMember[refs.length];
    int i = 0;
    for (long ref : refs) {
      ret[i++] = new OSMMember(ref, OSMType.NODE, -1);
    }
    return ret;
  }
}
