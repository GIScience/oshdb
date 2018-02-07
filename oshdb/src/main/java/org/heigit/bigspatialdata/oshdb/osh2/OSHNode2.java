package org.heigit.bigspatialdata.oshdb.osh2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;

public class OSHNode2 extends OSHEntity2 implements OSH<OSMNode> {
  
  protected OSHNode2(byte[] data, int offset, int length, byte header, long id, OSHDBBoundingBox bbox,
      long baseTimestamp, long baseLongitude, long baseLatitude, int[] keys, int dataOffset, int dataLength) {
    super(data, offset, length, header, id, bbox,  baseTimestamp, baseLongitude, baseLatitude, keys, dataOffset, dataLength);
  }

  @Override
  public OSMType type() {
    return OSMType.NODE;
  }

  @Override
  public OSHBuilder builder() {
    return new OSHNodeBuilder();
  }
  
  public static class OSHNodeBuilder extends OSHBuilder {
    private long longitude = 0;
    private long latitude = 0;
    
    private long minLon = Long.MAX_VALUE;
    private long maxLon = Long.MIN_VALUE;
    
    private long minLat = Long.MAX_VALUE;
    private long maxLat = Long.MIN_VALUE;
    
    @Override
    protected boolean extension(ByteArrayOutputWrapper out,OSMEntity version,long baseLongitude, long baseLatitude, Map<Long,Integer> nodeOffsets,Map<Long,Integer> wayOffsets,Map<Long,Integer> relationOffsets) throws IOException {
      OSMNode node = (OSMNode) version;
      final long lon = node.getLon() - baseLongitude;
      final long lat = node.getLat() - baseLatitude;
      if(lon != longitude || lat != latitude){
        longitude = out.writeSInt64Delta(lon, longitude);
        latitude  = out.writeSInt64Delta(lat, latitude);
        
        minLon = Math.min(minLon, lon);
        maxLon = Math.max(maxLon, lon);
        minLat = Math.min(minLat, lat);
        maxLat = Math.max(maxLat, lat);
        return true;
      }
      return false;
    }

    public long getMinLon() {
      return minLon;
    }

    public long getMaxLon() {
      return maxLon;
    }

    public long getMinLat() {
      return minLat;
    }

    public long getMaxLat() {
      return maxLat;
    }
  }
  
  @Override
  public Iterator<OSMNode> iterator() {
    return new OSMNodeIterator(data, dataOffset, dataLength, this);
  }
  public static class OSMNodeIterator extends OSMIterator<OSMNode> {
    public OSMNodeIterator(byte[] data, int offset, int length, OSHNode2 entity) {
      super(data, offset, length, entity);
    }

    private long longitude = 0;
    private long latitude = 0;
    
    @Override
    protected OSMNode extension() {
      try {        
        if (changedExtension()) {
          longitude = in.readSInt64Delta(longitude);
          latitude  = in.readSInt64Delta(latitude);
        }
        return new OSMNode(entity.id, version, new OSHDBTimestamp(entity.baseTimestamp + timestamp), changeset, userId, keyValues, //
            entity.baseLongitude + longitude, entity.baseLatitude + latitude);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
