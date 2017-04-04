package org.heigit.bigspatialdata.hosmdb.osh;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.builder.Builder;
import org.heigit.bigspatialdata.hosmdb.osm.OSMEntity;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.heigit.bigspatialdata.hosmdb.util.BoundingBox;
import org.heigit.bigspatialdata.hosmdb.util.ByteArrayOutputWrapper;
import org.heigit.bigspatialdata.hosmdb.util.ByteArrayWrapper;

public class HOSMNode extends HOSMEntity
    implements Iterable<OSMNode>, Serializable {

  private static final long serialVersionUID = 1L;
  
  private static final int CHANGED_USER_ID = 1 << 0;
  private static final int CHANGED_TAGS = 1 << 1;
  private static final int CHANGED_LOCATION = 1 << 2;

  private static final int HEADER_MULTIVERSION = 1 << 0;
  private static final int HEADER_TIMESTAMPS_NOT_IN_ORDER = 1 << 1;
  private static final int HEADER_HAS_TAGS = 1 << 2;
  private static final int HEADER_HAS_BOUNDINGBOX = 1 << 3;
  
  
  public static HOSMNode instance(final byte[] data, final int offset, final int length) throws IOException{
    return instance(data, offset, length, 0, 0, 0, 0);
  }
  
  public static HOSMNode instance(final byte[] data, final int offset, final int length, final long baseNodeId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude) throws IOException{
    
    ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, offset, length);
    final byte header = wrapper.readRawByte();
    final BoundingBox bbox;
    if ((header & HEADER_HAS_BOUNDINGBOX) != 0) {
      final long minLon = baseLongitude + wrapper.readSInt64();
      final long maxLon = minLon + wrapper.readUInt64();
      final long minLat = baseLatitude + wrapper.readSInt64();
      final long maxLat = minLat + wrapper.readUInt64();

      bbox = new BoundingBox(minLon * OSMNode.GEOM_PRECISION, maxLon * OSMNode.GEOM_PRECISION,
          minLat * OSMNode.GEOM_PRECISION, maxLat * OSMNode.GEOM_PRECISION);

    } else {
      bbox = null;
    }
    final int[] keys;
    if((header & HEADER_HAS_TAGS) != 0){
      final int size = wrapper.readUInt32();
      keys = new int[size];
      for(int i=0; i<size;i++){
        keys[i] = wrapper.readUInt32();
      }
    }else{
      keys = new int[0];
    }
    final long id = wrapper.readUInt64() + baseNodeId;
    final int dataOffset = wrapper.getPos();
    
    //TODO do we need dataLength?
    //TODO maybe better to store number of versions instead
    final int dataLength = length - (dataOffset - offset);
    
    return new HOSMNode(data, offset, length, baseNodeId, baseTimestamp, baseLongitude, baseLatitude, header, id, bbox, keys, dataOffset, dataLength);
  }
  
  private HOSMNode(final byte[] data, final int offset, final int length, 
      final long baseNodeId,final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final byte header, final long id, final BoundingBox bbox, final int[] keys,
      final int dataOffset, final int dataLength){
    super(data, offset, length, baseNodeId, baseTimestamp, baseLongitude, baseLatitude, header, id, bbox, keys, dataOffset, dataLength);
  }
  

  public List<OSMNode> getVersions() {
    List<OSMNode> versions = new ArrayList<>();
    this.forEach(versions::add);
    return versions;
  }
  
  @Override
  public Iterator<OSMNode> iterator() {
    return new Iterator<OSMNode>() {
      ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(data, dataOffset, dataLength);

      int version = 0;
      long timestamp = 0;
      long changeset = 0;
      int userId = 0;
      int[] keyValues = new int[0];

      private long longitude = 0;
      private long latitude = 0;

      @Override
      public boolean hasNext() {
        return wrapper.hasLeft() > 0;
      }

      @Override
      public OSMNode next() {
        try {
          version = wrapper.readSInt32() + version;
          timestamp = wrapper.readSInt64() + timestamp;
          changeset = wrapper.readSInt64() + changeset;

          byte changed = wrapper.readRawByte();

          if ((changed & CHANGED_USER_ID) != 0) {
            userId = wrapper.readSInt32() + userId;
          }

          if ((changed & CHANGED_TAGS) != 0) {
            int size = wrapper.readUInt32();
            if (size < 0) {
              System.out.println("Something went wrong!");
            }
            keyValues = new int[size];
            for (int i = 0; i < size; i++) {
              keyValues[i] = wrapper.readUInt32();
            }
          }

          if ((changed & CHANGED_LOCATION) != 0) {
            longitude = wrapper.readSInt64() + longitude;
            latitude = wrapper.readSInt64() + latitude;
          }

          return new OSMNode(id, version, baseTimestamp + timestamp, changeset, userId, keyValues,
              (version > 0) ? baseLongitude + longitude : 0,
              (version > 0) ? baseLatitude + latitude : 0);
        } catch (IOException e) {
          e.printStackTrace();
        }

        return null;
      }
    };
  }

  public static HOSMNode build(List<OSMNode> versions) throws IOException {
    return build(versions, 0, 0, 0, 0);
  }


  public static HOSMNode build(List<OSMNode> versions, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude)
      throws IOException {
    Collections.sort(versions, Collections.reverseOrder());
    ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();

    long lastLongitude = 0;
    long lastLatitude = 0;


    long id = versions.get(0).getId();

    long minLon = Long.MAX_VALUE;
    long maxLon = Long.MIN_VALUE;
    long minLat = Long.MAX_VALUE;
    long maxLat = Long.MIN_VALUE;
    

    Builder builder = new Builder(output, baseTimestamp);
    
    for (int i = 0; i < versions.size(); i++) {
      OSMNode node = versions.get(i);
      OSMEntity version = node;

      byte changed = 0;
      
      if (version.isVisible()
          && (node.getLon() != lastLongitude || node.getLat() != lastLatitude)) {
        changed |= CHANGED_LOCATION;
      }
      builder.build(version, changed);
      if ((changed & CHANGED_LOCATION) != 0) {
        output.writeSInt64(node.getLon() - lastLongitude - baseLongitude);
        lastLongitude = node.getLon();
        output.writeSInt64(node.getLat() - lastLatitude - baseLatitude);
        lastLatitude = node.getLat();

        minLon = Math.min(minLon, lastLongitude);
        maxLon = Math.max(maxLon, lastLongitude);

        minLat = Math.min(minLat, lastLatitude);
        maxLat = Math.max(maxLat, lastLatitude);
      }
    } // for versions

    ByteArrayOutputWrapper record = new ByteArrayOutputWrapper();

    byte header = 0;
    if (versions.size() > 1)
      header |= HEADER_MULTIVERSION;
    if (builder.getTimestampsNotInOrder())
      header |= HEADER_TIMESTAMPS_NOT_IN_ORDER;
    if (builder.getKeySet().size() > 0)
      header |= HEADER_HAS_TAGS;

    if (minLon != maxLon || minLat != maxLat) {
      header |= HEADER_HAS_BOUNDINGBOX;
    }

    record.writeByte(header);
    if ((header & HEADER_HAS_BOUNDINGBOX) != 0) {
      record.writeSInt64(minLon - baseLongitude);
      record.writeUInt64(maxLon - minLon);
      record.writeSInt64(minLat - baseLatitude);
      record.writeUInt64(maxLat - minLat);
    }
    
    if ((header & HEADER_HAS_TAGS) != 0) {
      record.writeUInt32(builder.getKeySet().size());
      for (Integer key : builder.getKeySet()) {
        record.writeUInt32(key.intValue());
      }
    }

    record.writeUInt64(id - baseId);
    record.writeByteArray(output.toByteArray());

    byte[] data = record.toByteArray();
    return HOSMNode.instance(data, 0, data.length,baseId, baseTimestamp, baseLongitude, baseLatitude);
  }

  @Override
  public HOSMNode rebase(long baseNodeId, long baseTimestamp2, long baseLongitude2,
      long baseLatitude2) throws IOException {
    List<OSMNode> nodes = getVersions();
    return HOSMNode.build(nodes, baseNodeId, baseTimestamp2, baseLongitude2, baseLatitude2);
  }

  public boolean hasTags() {
    return (header & HEADER_HAS_TAGS) != 0;
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Externalizable {

    private final HOSMNode node;
    private byte[] data;

    public SerializationProxy(HOSMNode node) {
      this.node = node;
    }

    public SerializationProxy() {
      this.node = null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(node.getLength());
      node.writeTo(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int length = in.readInt();
      data = new byte[length];
      in.readFully(data);
    }

    private Object readResolve() {
      try {
        return HOSMNode.instance(data, 0, data.length);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }
  }
}
