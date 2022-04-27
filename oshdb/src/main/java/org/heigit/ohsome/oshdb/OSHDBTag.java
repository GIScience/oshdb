package org.heigit.ohsome.oshdb;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * Key/Value id base OSM Tag class.
 *
 */
public class OSHDBTag implements Comparable<OSHDBTag>, Serializable {
  /**
   * Order by keyId/valueId, default Comparator for OSHDBTag.
   */
  public static final Comparator<OSHDBTag> ORDER_BY_ID = Comparator
      .comparingInt(OSHDBTag::getKey)
      .thenComparingInt(OSHDBTag::getValue);

  private static final long serialVersionUID = 1L;
  private final int key;
  private final int value;

  public OSHDBTag(int key, int value) {
    this.key = key;
    this.value = value;
  }

  public int getKey() {
    return this.key;
  }

  public int getValue() {
    return this.value;
  }

  public boolean isPresentInKeytables() {
    return this.value >= 0 && this.key >= 0;
  }

  @Override
  public int compareTo(OSHDBTag o) {
    return ORDER_BY_ID.compare(this, o);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof OSHDBTag
        && ((OSHDBTag) o).key == this.key && ((OSHDBTag) o).value == this.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.key, this.value);
  }

  @Override
  public String toString() {
    return Integer.toString(this.key) + "=" + Integer.toString(this.value);
  }
}
