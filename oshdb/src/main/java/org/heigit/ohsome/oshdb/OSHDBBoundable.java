package org.heigit.ohsome.oshdb;

import org.heigit.ohsome.oshdb.osm.OSMCoordinates;

/**
 * Interface for spatially boundable objects, i.e. objects which have a bounding box.
 */
public interface OSHDBBoundable {

  int getMinLon();

  int getMinLat();

  int getMaxLon();

  int getMaxLat();

  default double getMinLongitude() {
    return OSMCoordinates.toDouble(getMinLon());
  }

  default double getMinLatitude() {
    return OSMCoordinates.toDouble(getMinLat());
  }

  default double getMaxLongitude() {
    return OSMCoordinates.toDouble(getMaxLon());
  }

  default double getMaxLatitude() {
    return OSMCoordinates.toDouble(getMaxLat());
  }

  /**
   * Calculates the intersection between this and {@code other} {@code OSHDBBoundable}.
   *
   * @param other the {@code OSHDBBoundable}
   * @return the intersection between this and {@code other} {@code OSHDBBoundable}
   */
  default boolean intersects(OSHDBBoundable other) {
    return other != null
        && getMaxLat() >= other.getMinLat()
        && getMinLat() <= other.getMaxLat()
        && getMaxLon() >= other.getMinLon()
        && getMinLon() <= other.getMaxLon();
  }

  /**
   * Returns true if this {@code OSHDBBoundable} is inside/coveredBy the {@code other} object.
   *
   * @param other the {@code OSHDBBoundable} which is being checked for inside/coveredBy
   *          this {@code OSHDBBoundable}
   * @return {@code true} if the {@code OSHDBBoundable} is inside
   */
  default boolean coveredBy(OSHDBBoundable other) {
    return other != null
        && getMinLat() >= other.getMinLat()
        && getMaxLat() <= other.getMaxLat()
        && getMinLon() >= other.getMinLon()
        && getMaxLon() <= other.getMaxLon();
  }

  default boolean isPoint() {
    return getMinLon() == getMaxLon() && getMinLat() == getMaxLat();
  }

  default boolean isValid() {
    return getMinLon() <= getMaxLon() && getMinLat() <= getMaxLat();
  }

  /**
   * Calculates the intersection of this and {@code other} bounding boxes.
   *
   * @param other the bounding box for which to get the intersection
   * @return the intersection of the two bboxes
   */
  default OSHDBBoundingBox intersection(OSHDBBoundable other) {
    return OSHDBBoundingBox.bboxOSMCoordinates(
        Math.max(getMinLon(), other.getMinLon()),
        Math.max(getMinLat(), other.getMinLat()),
        Math.min(getMaxLon(), other.getMaxLon()),
        Math.min(getMaxLat(), other.getMaxLat()));
  }
}
