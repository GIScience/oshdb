package org.heigit.ohsome.oshdb.store;

import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;

public class OSHData {
   private final OSMType type;
   private final long id;

   private long gridId;
   private byte[] data;

  public OSHData(OSMType type, long id, long gridId, byte[] data) {
     this.type = type;
     this.id = id;
     this.gridId = gridId;
     this.data = data;
  }

  public OSMType getType() {
      return type;
   }

   public long getId() {
      return id;
   }

  public long getGridId() {
    return gridId;
  }

  public byte[] getData() {
    return data;
  }

  public <T extends OSHEntity> T getOSHEntity() {
    switch (type) {
      case NODE: return (T) OSHNodeImpl.instance(data, 0, 0);
      case WAY: return (T) OSHWayImpl.instance(data,0, 0);
      case RELATION: return (T) OSHRelationImpl.instance(data, 0, 0);
      default: throw new IllegalStateException();
    }
  }
}
