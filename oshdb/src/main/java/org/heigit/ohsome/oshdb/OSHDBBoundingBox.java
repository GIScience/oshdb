package org.heigit.ohsome.oshdb;

import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.GEOM_PRECISION_TO_LONG;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public class OSHDBBoundingBox implements OSHDBBoundable, Serializable {
  private static final long serialVersionUID = 1L;
  public static final OSHDBBoundingBox INVALID = bboxOSMCoordinates(1, 1, -1, -1);

  private final int minLon;
  private final int maxLon;
  private final int minLat;
  private final int maxLat;

  /**
   * Creates an {@code OSHDBBoundingBox} instance from osm long coordinates.
   */
  public static OSHDBBoundingBox bboxOSMCoordinates(int minLon, int minLat,
      int maxLon, int maxLat) {
    return new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat);
  }

  private OSHDBBoundingBox(int minLon, int minLat, int maxLon, int maxLat) {
    this.minLon = minLon;
    this.minLat = minLat;
    this.maxLon = maxLon;
    this.maxLat = maxLat;
  }

  /**
   * Create an {@code OSHDBBoudingBox} with standard double longitude/latitude coordinates.
   */
  public static OSHDBBoundingBox bboxLonLatCoordinates(double minLon, double minLat, double maxLon,
      double maxLat) {
    return bboxOSMCoordinates(
        Math.toIntExact(Math.round(minLon * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(minLat * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(maxLon * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(maxLat * GEOM_PRECISION_TO_LONG)));
  }

  @Override
  public int getMinLon() {
    return minLon;
  }

  @Override
  public int getMaxLon() {
    return maxLon;
  }

  @Override
  public int getMinLat() {
    return minLat;
  }

  @Override
  public int getMaxLat() {
    return maxLat;
  }

  public int[] getLon() {
    return new int[] {minLon, maxLon};
  }

  public int[] getLat() {
    return new int[] {minLat, maxLat};
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH,
        "(%3.7f,%3.7f,%3.7f,%3.7f)",
        this.getMinLongitude(),
        this.getMinLatitude(),
        this.getMaxLongitude(),
        this.getMaxLatitude());
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxLat, maxLon, minLat, minLon);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OSHDBBoundingBox)) {
      return false;
    }
    OSHDBBoundingBox other = (OSHDBBoundingBox) obj;
    return maxLat == other.maxLat && maxLon == other.maxLon && minLat == other.minLat
        && minLon == other.minLon;
  }
}
