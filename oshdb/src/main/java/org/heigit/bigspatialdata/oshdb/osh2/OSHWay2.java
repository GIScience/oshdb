package org.heigit.bigspatialdata.oshdb.osh2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;

public abstract class OSHWay2 extends OSHEntity2 implements OSH<OSMWay> {

  protected OSHWay2(byte[] data, int offset, int length, byte header, long id, OSHDBBoundingBox bbox,
      long baseTimestamp, long baseLongitude, long baseLatitude, int[] keys, int dataOffset, int dataLength) {
    super(data, offset, length, header, id, bbox, baseTimestamp, baseLongitude, baseLatitude, keys, dataOffset,dataLength);
  }

  @Override
  public OSMType type() {
    return OSMType.WAY;
  }


  @Override
  public OSHBuilder builder() {
    return new OSHWayBuilder();
  }

  public static class OSHWayBuilder extends OSHBuilder {
    private OSMMember[] members = new OSMMember[0];

    @Override
    protected boolean extension(ByteArrayOutputWrapper out,OSMEntity version,long baseLongitude, long baseLatitude, 
        Map<Long, Integer> nodeOffsets,Map<Long, Integer> wayOffsets, Map<Long, Integer> relationOffsets) throws IOException {
      OSMWay way = (OSMWay) version;
      if (!memberEquals(way.getRefs(), members)) {
        members = way.getRefs();
        out.writeUInt32(members.length);
        long lastId = 0;
        for (OSMMember member : members) {
          final long memId = member.getId();
          final Integer memberOffset = nodeOffsets.get(Long.valueOf(memId));

          if (memberOffset == null) {
            lastId = out.writeSInt64Delta((memId * -1),lastId);
          } else {
            long offset = memberOffset.longValue();
            lastId = out.writeSInt64Delta(offset,lastId);
          }
        }
        return true;
      }
      return false;
    }

    private boolean memberEquals(OSMMember[] a, OSMMember[] b) {
      if (a.length != b.length) {
        return false;
      }
      for (int i = 0; i < a.length; i++) {
        if (a[i].getId() != b[i].getId()) {
          return false;
        }
      }
      return true;
    }
  }
  
  public abstract OSMMember getMember(long memId);

  @Override
  public Iterator<OSMWay> iterator() {
    return new OSMWayIterator(data, dataOffset, dataLength, this);
  }

  public static class OSMWayIterator extends OSMIterator<OSMWay> {
    
    
    public OSMWayIterator(byte[] data, int offset, int length, OSHWay2 way) {
      super(data, offset, length, way);
      this.way = way;
    }
    
    private final OSHWay2 way;
    private OSMMember[] members = new OSMMember[0];

    @Override
    protected OSMWay extension() {
      try {
        if (changedExtension()) {
          final int length = in.readUInt32();
          members = new OSMMember[length];

          long memId = 0;
          for (int i = 0; i < length; i++) {
            memId = in.readSInt64Delta(memId);
            members[i] = way.getMember(memId);
          }
        }
        return new OSMWay(entity.id, version, new OSHDBTimestamp(entity.baseTimestamp + timestamp), changeset, userId, keyValues, members);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
