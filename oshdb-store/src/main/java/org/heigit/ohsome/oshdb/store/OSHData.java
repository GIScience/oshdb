package org.heigit.ohsome.oshdb.store;

import java.io.Serializable;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;

public class OSHData implements Serializable {
   private final OSMType type;
   private final long id;

   private long gridId;
   private byte[] data;

   private transient OSHEntity osh;

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
    if (osh == null) {
      osh = oshEntity();
    }
    return (T) osh;
  }

  private OSHEntity oshEntity(){
    switch (type) {
      case NODE: return OSHNodeImpl.instance(data, 0, 0);
      case WAY: return OSHWayImpl.instance(data,0, 0);
      case RELATION: return OSHRelationImpl.instance(data, 0, 0);
      default: throw new IllegalStateException();
    }
  }
}
