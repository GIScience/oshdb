package org.heigit.bigspatialdata.oshdb.tool.importer.transform.oshdb;

public class MutableOSMNode extends MutableOSMEntity implements OSMNode {
  private long longitude;
  private long latitude;
  
  @Override
  public long getLon() {
    return longitude;
  }
  
  public void setLon(long longitude){
    this.longitude = longitude;
  }
  

  @Override
  public long getLat() {
    return latitude;
  }
  
  public void setLat(long latitude){
    this.latitude = latitude;
  }

  public void setExtension(long longitude, long latitude) {
    this.longitude = longitude;
    this.latitude = latitude;
  }

}
