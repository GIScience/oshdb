package org.heigit.ohsome.oshdb;

/**
 * Interface for spatially boundable objects, i.e. objects which have a bounding box.
 */
public interface OSHDBBoundable {

  /**
   * Returns the minimum longitude in osm-coordinate-system.
   *
   * @return minimum longitude
   */
  int getMinLongitude();

  /**
   * Returns the minimum latitude in osm-coordinate-system.
   *
   * @return minimum latitude
   */
  int getMinLatitude();

  /**
   * Returns the maximum longitude in osm-coordinate-system.
   *
   * @return maximum longitude
   */
  int getMaxLongitude();

  /**
   * Returns the maximum latitude in osm-coordinate-system.
   *
   * @return maximum latitude
   */
  int getMaxLatitude();

  /**
   * Calculates the intersection between this and {@code other} {@code OSHDBBoundable}.
   *
   * @param other the {@code OSHDBBoundable}
   * @return the intersection between this and {@code other} {@code OSHDBBoundable}
   */
  default boolean intersects(OSHDBBoundable other) {
    return other != null
        && getMaxLatitude() >= other.getMinLatitude()
        && getMinLatitude() <= other.getMaxLatitude()
        && getMaxLongitude() >= other.getMinLongitude()
        && getMinLongitude() <= other.getMaxLongitude();
  }

  /**
   * Returns true if this {@code OSHDBBoundable} is inside/coveredBy the {@code other} object.
   *
   * @param other the {@code OSHDBBoundable} which is being checked for inside/coveredBy this
   *        {@code OSHDBBoundable}
   * @return {@code true} if the {@code OSHDBBoundable} is inside
   */
  default boolean coveredBy(OSHDBBoundable other) {
    return other != null
        && getMinLatitude() >= other.getMinLatitude()
        && getMaxLatitude() <= other.getMaxLatitude()
        && getMinLongitude() >= other.getMinLongitude()
        && getMaxLongitude() <= other.getMaxLongitude();
  }

  /**
   * Checks if this {@link OSHDBBoundable} collapsed to a single point.
   *
   * @return true if collapsed to a single point
   */
  default boolean isPoint() {
    return getMinLongitude() == getMaxLongitude() && getMinLatitude() == getMaxLatitude();
  }

  /**
   * Checks if this {@link OSHDBBoundable} describes a valid boundable.
   *
   * @return true, if this {@link OSHDBBoundable} is valid
   */
  default boolean isValid() {
    return getMinLongitude() <= getMaxLongitude() && getMinLatitude() <= getMaxLatitude();
  }

  /**
   * Calculates the intersection of this and {@code other} bounding boxes.
   *
   * @param other the bounding box for which to get the intersection
   * @return the intersection of the two bboxes
   */
  default OSHDBBoundingBox intersection(OSHDBBoundable other) {
    return OSHDBBoundingBox.bboxOSMCoordinates(
        Math.max(getMinLongitude(), other.getMinLongitude()),
        Math.max(getMinLatitude(), other.getMinLatitude()),
        Math.min(getMaxLongitude(), other.getMaxLongitude()),
        Math.min(getMaxLatitude(), other.getMaxLatitude()));
  }

  /**
   * Creates a new OSHDBBoundingBox object.
   *
   * @return new OSHDBBoundingBox object.
   */
  default OSHDBBoundingBox getBoundingBox() {
    return OSHDBBoundingBox.bboxWgs84Coordinates(getMinLongitude(), getMinLatitude(),
        getMaxLongitude(), getMaxLatitude());
  }
}
