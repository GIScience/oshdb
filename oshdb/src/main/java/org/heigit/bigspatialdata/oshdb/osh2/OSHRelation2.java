package org.heigit.bigspatialdata.oshdb.osh2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;

public abstract class OSHRelation2 extends OSHEntity2 implements OSH<OSMRelation> {


  protected OSHRelation2(byte[] data, int offset, int length, byte header, long id, OSHDBBoundingBox bbox,
      long baseTimestamp, long baseLongitude, long baseLatitude, int[] keys, int dataOffset, int dataLength) {
    super(data, offset, length, header, id, bbox, baseTimestamp, baseLongitude, baseLatitude, keys, dataOffset,
        dataLength);

    
  }

  @Override
  public OSMType type() {
    return OSMType.RELATION;
  }

  @Override
  public OSHBuilder builder() {
    return new OSHRelationBuilder();
  }

  public static class OSHRelationBuilder extends OSHBuilder {
    private OSMMember[] members = new OSMMember[0];

    @Override
    protected boolean extension(ByteArrayOutputWrapper out,OSMEntity version,long baseLongitude, long baseLatitude, Map<Long, Integer> nodeOffsets,
        Map<Long, Integer> wayOffsets, Map<Long, Integer> relationOffsets) throws IOException {
      OSMRelation relation = (OSMRelation) version;
      if (!memberEquals(relation.getMembers(), members)) {
        members = relation.getMembers();
        out.writeUInt32(members.length);
        long lastId = 0;
        for (OSMMember member : members) {
          final long memId = member.getId();
          final OSMType type = member.getType();
          final int typeId = type.intValue();
          final int role = member.getRawRoleId();
          final Integer memberOffset;
          if (type == OSMType.NODE)
            memberOffset = nodeOffsets.get(Long.valueOf(member.getId()));
          else if (type == OSMType.WAY)
            memberOffset = wayOffsets.get(Long.valueOf(member.getId()));
          else
            memberOffset = null;

          if (memberOffset == null) {
            out.writeSInt32(typeId * -1);
            lastId = out.writeSInt64Delta(memId,lastId);
          } else {
            out.writeSInt32(typeId);
            long offset = memberOffset.longValue();
            lastId = out.writeSInt64Delta(offset,lastId);
          }
          out.writeUInt32(role);

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
        if (a[i].getId() != b[i].getId())
          return false;
        if (a[i].getType() != b[i].getType())
          return false;
        if (a[i].getRawRoleId() != b[i].getRawRoleId())
          return false;
      }
      return true;
    }

  }

  
  public abstract OSMMember getMember(long memId,int type, int role);
  
  @Override
  public Iterator<OSMRelation> iterator() {
    return new OSMRelationIterator(data, dataOffset, dataLength, this);
  }

    
  public static class OSMRelationIterator extends OSMIterator<OSMRelation> {
    public OSMRelationIterator(byte[] data, int offset, int length, OSHRelation2 relation) {
      super(data, offset, length, relation);
      this.relation = relation;
    }

    private final OSHRelation2 relation;
    private OSMMember[] members = new OSMMember[0];

    @Override
    protected OSMRelation extension() {
      try {
        if (changedExtension()) {
          final int length = in.readUInt32();
          members = new OSMMember[length];

          long memId = 0;
          for (int i = 0; i < length; i++) {
            final int type = in.readSInt32();
            memId = memId + in.readSInt64();
            final int role = in.readUInt32();

            members[i] = relation.getMember(memId, type, role);
            
          }
        }
        return new OSMRelation(entity.id, version, new OSHDBTimestamp(entity.baseTimestamp + timestamp), changeset, userId, keyValues,
            members);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
