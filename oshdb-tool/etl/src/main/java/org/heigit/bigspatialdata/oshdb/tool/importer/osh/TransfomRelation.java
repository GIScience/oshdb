package org.heigit.bigspatialdata.oshdb.tool.importer.osh;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.osh2.OSHRelation2;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class TransfomRelation extends OSHRelation2 {
  
  public static TransfomRelation build(ByteArrayOutputWrapper output, ByteArrayOutputWrapper record,
      ByteArrayOutputWrapper aux, 
      List<OSMRelation> versions, 
      LongSortedSet nodeIds, LongSortedSet wayIds, 
      final long baseId, final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {
    Collections.sort(versions, Collections.reverseOrder());

    output.reset();
    record.reset();
    aux.reset();
    
    final long id = versions.get(0).getId();
    
//    byte header = 0;
    //record.writeByte(header);

    record.writeUInt64(id - baseId);
    
    Map<Long, Integer> nodeOffsets = memberOffsets(nodeIds, record);
    Map<Long, Integer> wayOffsets = memberOffsets(wayIds, record);
    
    OSHRelation2.OSHRelationBuilder builder =  new OSHRelation2.OSHRelationBuilder();
    builder.build(output, aux, versions, baseTimestamp, baseLongitude, baseLatitude, nodeOffsets, wayOffsets, Collections.emptyMap());

    record.writeByteArray(output.array(), 0, output.length());
    return TransfomRelation.instance(record.array(), 0, record.length(),baseId,baseTimestamp,baseLongitude,baseLatitude);
  }
  
  private static Map<Long, Integer> memberOffsets(LongSortedSet ids, ByteArrayOutputWrapper out) throws IOException{
    out.writeUInt32(ids.size());
    Map<Long, Integer> offsets = new HashMap<>(ids.size());
    long nodeId = 0;
    LongIterator itr = ids.iterator();
    for(int i=0; itr.hasNext(); i++){
      nodeId = out.writeUInt64Delta(itr.nextLong(),nodeId);
      offsets.put(nodeId, i);
    }
    return offsets;
  }
  
  private static long[] readMemberIds(ByteArrayWrapper wrapper) throws IOException{
    final long[] nodeIds = new long[wrapper.readUInt32()];    
    long nodeId = 0;
    for (int i = 0; i < nodeIds.length; i++) {
      nodeId = wrapper.readUInt64Delta(nodeId);
      nodeIds[i] = nodeId;
    }
    return nodeIds;
  }
  
  public static TransfomRelation instance(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {

    final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    
    final byte header = 0; //wrapper.readRawByte();
    final long id = wrapper.readUInt64() + baseId;

    final long[] nodeIds = readMemberIds(wrapper);
    final long[] wayIds = readMemberIds(wrapper);

    final int dataOffset = wrapper.getPos();
    final int dataLength = length - (dataOffset - offset);
    return new TransfomRelation(data, offset, length, header, id, 
        baseTimestamp, baseLongitude, baseLatitude,        
        dataOffset, dataLength, 
        nodeIds,wayIds);
  }
  
  public static TransfomRelation instance(final byte[] data, final int offset, final int length) throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }
  

  final Map<OSMType,long[]> offsetToId;
  
  protected TransfomRelation(byte[] data, int offset, int length, byte header, long id, 
      long baseTimestamp, long baseLongitude, long baseLatitude, int dataOffset, int dataLength, long[] nodeIds, long[] wayIds) {
    super(data, offset, length, header, id, OSHDBBoundingBox.EMPTY, baseTimestamp, baseLongitude, baseLatitude, new int[0], dataOffset, dataLength);

    offsetToId = new HashMap<>(2);
    offsetToId.put(OSMType.NODE,nodeIds);
    offsetToId.put(OSMType.WAY,wayIds);
  }

  @Override
  public OSMMember getMember(long memId, int type, int role) {
    if (type < 0)
      return new OSMMember(memId, OSMType.fromInt(type * -1), role);
    else {
      OSMType osmType = OSMType.fromInt(type);
      long id = offsetToId.get(osmType)[(int) memId];
      return new OSMMember(id, OSMType.fromInt(type), role);
    }
  }

  public long[] getNodeIds(){
    return offsetToId.get(OSMType.NODE);
  }
  
  public long[] getWayIds(){
    return offsetToId.get(OSMType.WAY);
  }
  
}
