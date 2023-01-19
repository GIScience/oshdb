package org.heigit.ohsome.oshdb.osm;

import java.util.Comparator;
import java.util.Objects;

public class OSMId implements Comparable<OSMId>{
  public static final Comparator<OSMId> NATURAL_ORDER = Comparator.comparing(OSMId::getType).thenComparingLong(OSMId::getId);


  private final OSMType type;
  private final long id;

  public OSMId(OSMType type, long id) {
    this.type = type;
    this.id = id;
  }

  public OSMType getType() {
    return type;
  }

  public long getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OSMId)) {
      return false;
    }
    OSMId osmId = (OSMId) o;
    return id == osmId.id && type == osmId.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id);
  }

  @Override
  public String toString() {
    return type + "/" + id;
  }

  @Override
  public int compareTo(OSMId o) {
    return NATURAL_ORDER.compare(this, o);
  }
}
