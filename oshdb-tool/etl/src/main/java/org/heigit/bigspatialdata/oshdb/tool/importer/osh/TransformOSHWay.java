package org.heigit.bigspatialdata.oshdb.tool.importer.osh;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.osh2.OSHWay2;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;

import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class TransformOSHWay extends OSHWay2 {

  private final long[] nodeIds;

  public static TransformOSHWay build(ByteArrayOutputWrapper output, ByteArrayOutputWrapper record,
      ByteArrayOutputWrapper aux, List<OSMWay> versions, LongSortedSet nodeIds, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {
    Collections.sort(versions, Collections.reverseOrder());

    output.reset();
    record.reset();
    aux.reset();

    final long id = versions.get(0).getId();

//    byte header = 0;
//    record.writeByte(header);
    record.writeUInt64Delta(id, baseId);
    
    Map<Long, Integer> nodeOffsets = writeNodeIds(nodeIds, record);
    
    OSHWay2.OSHWayBuilder builder = new OSHWay2.OSHWayBuilder();
    builder.build(output, aux, versions, baseTimestamp, baseLongitude, baseLatitude, nodeOffsets, Collections.emptyMap(), Collections.emptyMap());
    
    record.writeByteArray(output.array(), 0, output.length());
    return TransformOSHWay.instance(record.array(), 0, record.length(),baseId,baseTimestamp,baseLongitude,baseLatitude);
  }
  
  private static Map<Long, Integer> writeNodeIds(LongSortedSet ids, ByteArrayOutputWrapper out) throws IOException{
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
  
  private static long[] readNodeIds(ByteArrayWrapper wrapper) throws IOException{
    final long[] nodeIds = new long[wrapper.readUInt32()];    
    long nodeId = 0;
    for (int i = 0; i < nodeIds.length; i++) {
      nodeId = wrapper.readUInt64Delta(nodeId);
      nodeIds[i] = nodeId;
    }
    return nodeIds;
  }

  public static TransformOSHWay instance(final byte[] data, final int offset, final int length) throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  public static TransformOSHWay instance(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {

    final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    
    final byte header = 0; //wrapper.readRawByte();
    final long id = wrapper.readUInt64() + baseId;

    final long[] nodeIds = readNodeIds(wrapper);

    final int dataOffset = wrapper.getPos();
    final int dataLength = length - (dataOffset - offset);
    return new TransformOSHWay(data, offset, length, header, id, 
        baseTimestamp, baseLongitude, baseLatitude,        
        dataOffset, dataLength, 
        nodeIds);
  }

  private TransformOSHWay(final byte[] data, final int offset, final int length, byte header, final long id, final long baseTimestamp,final long baseLongitude, final long baseLatitude, final int dataOffset, final int dataLength, final long[] nodeIds) {
    super(data, offset, length, header, id, OSHDBBoundingBox.EMPTY, 
        baseTimestamp, baseLongitude, baseLatitude,
        new int[0], 
        dataOffset, dataLength);
    this.nodeIds = nodeIds;
  }

  @Override
  public OSMMember getMember(long memId) {
    if (memId < 0)
      return new OSMMember(memId * -1, OSMType.NODE, -1);
    else {
      long id = nodeIds[(int) memId];
      return new OSMMember(id, OSMType.NODE, -1);
    }
  }
  
  public long[] getNodeIds() {
    return nodeIds;
  }
  
}
