package org.heigit.ohsome.oshdb.store;

import org.heigit.ohsome.oshdb.osm.OSMType;

public class OSHDBData {
  private final OSMType type;
  private final long id;

  private long gridId;
  private byte[] data;

  public static OSHDBData of(OSMType type, long id, long gridId, byte[] data) {
    return new OSHDBData(type, id).setData(gridId, data);
  }

  private OSHDBData(OSMType type, long id) {
    this.type = type;
    this.id = id;
  }

  private OSHDBData setData(long gridId, byte[] data) {
    this.gridId = gridId;
    this.data = data;
    return this;
  }

  public OSMType getType(){
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
