package org.heigit.ohsome.oshdb.store;

import java.util.Objects;
import org.heigit.ohsome.oshdb.osm.OSMType;

public class BackRef {
  private final OSMType type;
  private final long id;

  public BackRef(OSMType type, long id) {
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
    if (!(o instanceof BackRef)) {
      return false;
    }
    BackRef backRef = (BackRef) o;
    return id == backRef.id && type == backRef.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id);
  }

  @Override
  public String toString() {
    return "BackRef " + type + "/" + id;
  }
}
