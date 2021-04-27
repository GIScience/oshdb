package org.heigit.ohsome.oshdb;

public interface OSHDBBoundable {

  long getMinLonLong();

  long getMinLatLong();

  long getMaxLonLong();

  long getMaxLatLong();

  default double getMinLon() {
    return getMinLonLong() * OSHDB.GEOM_PRECISION;
  }

  default double getMinLat() {
    return getMinLatLong() * OSHDB.GEOM_PRECISION;
  }

  default double getMaxLon() {
    return getMaxLonLong() * OSHDB.GEOM_PRECISION;
  }

  default double getMaxLat() {
    return getMaxLatLong() * OSHDB.GEOM_PRECISION;
  }

  /**
   * Calculates the intersection between this and {@code other} {@code OSHDBBoundable}.
   *
   * @param other the {@code OSHDBBoundable}
   * @return the intersection between this and {@code other} {@code OSHDBBoundable}
   */
  default boolean intersects(OSHDBBoundable other) {
    return (other != null)
        && (getMaxLatLong() >= other.getMinLatLong())
        && (getMinLatLong() <= other.getMaxLatLong())
        && (getMaxLonLong() >= other.getMinLonLong())
        && (getMinLonLong() <= other.getMaxLonLong());
  }

  /**
   * Returns true if this {@code OSHDBBoundable} is inside/coveredBy the {@code other} object.
   * @param other the {@code OSHDBBoundable} which is being checked for inside/coveredBy
   *          this {@code OSHDBBoundable}
   * @return {@code true} if the {@code OSHDBBoundable} is inside
   */
  default boolean coveredBy(OSHDBBoundable other) {
    return (other != null)
        && (getMinLatLong() >= other.getMinLatLong())
        && (getMaxLatLong() <= other.getMaxLatLong())
        && (getMinLonLong() >= other.getMinLonLong())
        && (getMaxLonLong() <= other.getMaxLonLong());
  }

  default boolean isPoint() {
    return getMinLonLong() == getMaxLonLong() && getMinLatLong() == getMaxLatLong();
  }

  default boolean isValid() {
    return getMinLonLong() <= getMaxLonLong() && getMinLatLong() <= getMaxLatLong();
  }

  /**
   * Calculates the intersection of this and {@code other} bounding boxes.
   *
   * @param other the bounding box for which to get the intersection
   * @return the intersection of the two bboxes
   */
  default OSHDBBoundingBox intersection(OSHDBBoundable other) {
    return new OSHDBBoundingBox(
        Math.max(getMinLonLong(), other.getMinLonLong()),
        Math.max(getMinLatLong(), other.getMinLatLong()),
        Math.min(getMaxLonLong(), other.getMaxLonLong()),
        Math.min(getMaxLatLong(), other.getMaxLatLong()));
  }
}
