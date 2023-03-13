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

   private final long gridId;
   private final byte[] data;

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

  @SuppressWarnings("unchecked")
  public <T extends OSHEntity> T getOSHEntity() {
    if (osh == null) {
      osh = oshEntity();
    }
    return (T) osh;
  }

  private OSHEntity oshEntity(){
    return switch (type) {
      case NODE -> OSHNodeImpl.instance(data, 0, 0);
      case WAY -> OSHWayImpl.instance(data, 0, 0);
      case RELATION -> OSHRelationImpl.instance(data, 0, 0);
    };
  }

  @Override
  public String toString() {
    return "OSHData{" +
        "type=" + type +
        ", id=" + id +
        ", gridId=" + gridId +
        ", data=" + data.length +
        ", osh=" + osh +
        '}';
  }
}
