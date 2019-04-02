package org.heigit.bigspatialdata.oshdb.util.xmlreader;

public class MutableOSMNode extends MutableOSMEntity  {
  private long longitude;
  private long latitude;
  
  public long getLon() {
    return longitude;
  }
  
  public void setLon(long longitude){
    this.longitude = longitude;
  }
  

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
