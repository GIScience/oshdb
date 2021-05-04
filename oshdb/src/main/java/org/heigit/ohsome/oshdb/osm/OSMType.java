package org.heigit.ohsome.oshdb.osm;

public enum OSMType {
  NODE(0), WAY(1), RELATION(2);

  private final int value;

  OSMType(final int value) {
    this.value = value;
  }

  /**
   * Returns an {@code OSMType} instance represented by the given integer value (0-2), or throws an
   * exception otherwise.
   */
  public static OSMType fromInt(final int value) {
    switch (value) {
      case 0:
        return NODE;
      case 1:
        return WAY;
      case 2:
        return RELATION;
      default: {
        final String msg =
            String.format("Unknown OSMType! Should be between 0 and 2, got [%d]", value);
        throw new IllegalArgumentException(msg);
      }
    }
  }

  public final int intValue() {
    return this.value;
  }
}
