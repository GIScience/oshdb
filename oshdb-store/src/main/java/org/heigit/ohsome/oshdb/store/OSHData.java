package org.heigit.ohsome.oshdb.store;

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
}
