package org.heigit.bigspatialdata.oshdb.tool.importer.osh;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.osh2.OSHNode2;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;


public class TransformOSHNode extends OSHNode2 {
  
  public static TransformOSHNode build(ByteArrayOutputWrapper output,ByteArrayOutputWrapper record,ByteArrayOutputWrapper aux, List<OSMNode> versions, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude) throws IOException {
    Collections.sort(versions, Collections.reverseOrder());
       
    output.reset();
    record.reset();
    
    final long id = versions.get(0).getId();

    OSHNode2.OSHNodeBuilder builder = new OSHNode2.OSHNodeBuilder();
    
    builder.build(output, aux,versions,baseTimestamp,baseLongitude, baseLatitude, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

//    byte header = 0;
//    record.writeByte(header);
  
    record.writeUInt64(id - baseId);
    record.writeByteArray(output.array(),0,output.length());
    
    //final byte[] bytes = new byte[record.length()];
    //System.arraycopy(record.array(), 0, bytes, 0, bytes.length);
    
    return TransformOSHNode.instance(record.array(), 0, record.length(), baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  public static TransformOSHNode instance(final byte[] data, final int offset, final int length, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException {

    final ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
   
    final byte header =  0; //wrapper.readRawByte();
   
    final long id = wrapper.readUInt64() + baseId;
    final int dataOffset = wrapper.getPos();  
    final int dataLength = length - (dataOffset - offset);

    return new TransformOSHNode(data, offset, length,header,id, baseTimestamp, baseLongitude, baseLatitude,dataOffset, dataLength);
  }
  
  public static TransformOSHNode instance(final byte[] data, final int offset, final int length) throws IOException {
    return instance(data, offset, length, 0, 0, 0, 0);
  }

  private TransformOSHNode(final byte[] data, final int offset, final int length, final byte header, final long id,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude, final int dataOffset, final int dataLength) {
    super(data, offset, length, header,id, OSHDBBoundingBox.EMPTY, baseTimestamp, baseLongitude, baseLatitude, new int[0], dataOffset, dataLength);
  }
}
