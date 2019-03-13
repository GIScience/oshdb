package org.heigit.bigspatialdata.oshdb.util.tagtranslator;

public class OSMTag implements OSMTagInterface {
  private String key;
  private String value;

  public OSMTag(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return this.key;
  }

  public String getValue() {
    return this.value;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSMTag &&
        ((OSMTag)o).key.equals(this.key) && ((OSMTag)o).value.equals(this.value);
  }

  @Override
  public int hashCode() {
    return this.key.hashCode() + this.value.hashCode();
  }

  @Override
  public String toString() {
    return this.key + "=" + this.value;
  }
}
