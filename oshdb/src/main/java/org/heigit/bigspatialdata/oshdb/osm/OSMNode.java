package org.heigit.bigspatialdata.oshdb.osm;

import java.io.Serializable;
import java.util.Locale;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;

public class OSMNode extends OSMEntity implements Comparable<OSMNode>, Serializable {

  private static final long serialVersionUID = 1L;

  private final long longitude;
  private final long latitude;

  public OSMNode(final long id, final int version, final OSHDBTimestamp timestamp, final long changeset,
      final int userId, final int[] tags, final long longitude, final long latitude) {
    super(id, version, timestamp, changeset, userId, tags);
    this.longitude = longitude;
    this.latitude = latitude;
  }

  @Override
  public OSMType getType() {
    return OSMType.NODE;
  }

  public double getLongitude() {
    return longitude * OSHDB.GEOM_PRECISION;
  }

  public double getLatitude() {
    return latitude * OSHDB.GEOM_PRECISION;
  }

  public long getLon() {
    return longitude;
  }

  public long getLat() {
    return latitude;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH, "NODE: %s %.7f:%.7f", super.toString(), getLongitude(), getLatitude());
  }


  public boolean equalsTo(OSMNode o) {
    return super.equalsTo(o) && longitude == o.longitude && latitude == o.latitude;
  }

  @Override
  public int compareTo(OSMNode o) {
    int c = Long.compare(id, o.id);
    if (c == 0) {
      c = Integer.compare(Math.abs(version), Math.abs(o.version));
    }
    if (c == 0) {
      c = Long.compare(timestamp.getRawUnixTimestamp(), o.timestamp.getRawUnixTimestamp());
    }
    return c;
  }


}
