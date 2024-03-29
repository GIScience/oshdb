package org.heigit.ohsome.oshdb.util.tagtranslator;

/**
 * Represents an OSM tag key (which can be an arbitrary string).
 */
public class OSMTagKey implements OSMTagInterface {
  private String key;

  public OSMTagKey(String key) {
    this.key = key;
  }

  @Override
  public String toString() {
    return this.key;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSMTagKey && ((OSMTagKey) o).key.equals(this.key);
  }

  @Override
  public int hashCode() {
    return this.key.hashCode();
  }
}
