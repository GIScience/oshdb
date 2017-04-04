package org.heigit.bigspatialdata.hosmdb.osm;

import java.io.Serializable;

public class OSMNode extends OSMEntity implements Comparable<OSMNode>, Serializable {

  public static final double GEOM_PRECISION = .000000001;

  private static final long serialVersionUID = 1L;

  private final long longitude;
  private final long latitude;


  public OSMNode(final long id, final int version, final long timestamp, final long changeset,
      final int userId, final int[] tags, final long longitude, final long latitude) {
    super(id, version, timestamp, changeset, userId, tags);
    this.longitude = longitude;
    this.latitude = latitude;
  }


  public double getLongitude() {
    return longitude * GEOM_PRECISION;
  }


  public double getLatitude() {
    return latitude * GEOM_PRECISION;
  }

  public long getLon() {
    return longitude;
  }

  public long getLat() {
    return latitude;
  }

  @Override
  public String toString() {
    return String.format("NODE: %s %d:%d", super.toString(), getLon(), getLat());
  }

  public boolean equalsTo(OSMNode o){
    return super.equalsTo(o) && longitude == o.longitude && latitude == o.latitude;
  }

  @Override
  public int compareTo(OSMNode o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
    }
    if (c == 0) {
      c = Long.compare(timestamp, o.timestamp);
    }
    return c;
  }

}
