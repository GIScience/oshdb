package org.heigit.ohsome.oshdb.util.xmlreader;

/**
 * A mutable OSM node, specifically for use in {@link OSMXmlReader}.
 */
public class MutableOSMNode extends MutableOSMEntity  {
  private int longitude;
  private int latitude;

  public int getLon() {
    return longitude;
  }

  public void setLon(int longitude) {
    this.longitude = longitude;
  }

  public int getLat() {
    return latitude;
  }

  public void setLat(int latitude) {
    this.latitude = latitude;
  }

  public void setExtension(int longitude, int latitude) {
    this.longitude = longitude;
    this.latitude = latitude;
  }
}
