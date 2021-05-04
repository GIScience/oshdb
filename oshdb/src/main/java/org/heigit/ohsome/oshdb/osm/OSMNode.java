package org.heigit.ohsome.oshdb.osm;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import org.heigit.ohsome.oshdb.OSHDB;

public class OSMNode extends OSMEntity implements Comparable<OSMNode>, Serializable {

  private static final long serialVersionUID = 1L;

  private final long longitude;
  private final long latitude;

  /**
   * Creates a new {@code OSMNode} instance.
   */
  public OSMNode(final long id, final int version, final long timestamp, final long changeset,
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
    return String.format(Locale.ENGLISH, "NODE: %s %.7f:%.7f", super.toString(), getLongitude(),
        getLatitude());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Objects.hash(latitude, longitude);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof OSMNode)) {
      return false;
    }
    OSMNode other = (OSMNode) obj;
    return latitude == other.latitude && longitude == other.longitude;
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
