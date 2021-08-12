package org.heigit.ohsome.oshdb;

import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.GEOM_PRECISION_TO_LONG;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;

/**
 * This class describes a BoundingBox with min/max longitude/latitude.
 */
public class OSHDBBoundingBox implements OSHDBBoundable, Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Singleton invalid bounding box.
   */
  public static final OSHDBBoundingBox INVALID = bboxOSMCoordinates(1, 1, -1, -1);

  private final int minLon;
  private final int maxLon;
  private final int minLat;
  private final int maxLat;

  /**
   * Creates an {@code OSHDBBoundingBox} instance from scaled coordinates.
   *
   * <p>This method is mainly for internal usage.<br>
   * OSM stores coordinates with a fixed precision of
   * <a href="https://wiki.openstreetmap.org/wiki/Node#Structure">7 decimal
   * digits</a> and stores them internally as integers. You can use this method
   * to create a bounding box from such (scaled and rounded) coordinates. <br>
   * See {@code bboxWgs84Coordinates} for a wgs84 alternative.</p>
   *
   * @param minLon minimum longitude in osm-coordinate system
   * @param minLat minimum latitude in osm-coordinate system
   * @param maxLon maximum longitude in osm-coordinate system
   * @param maxLat maximum latitude in osm-coordinate system
   * @return new instance of {@link OSHDBBoundingBox}
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
   * Creates an {@code OSHDBBoundingBox} from wgs84 coordinates.
   *
   * @param minLon minimum longitude in wgs84 coordinate system
   * @param minLat minimum latitude in wgs84 coordinate system
   * @param maxLon maximum longitude in wgs84 coordinate system
   * @param maxLat maximum latitude in wgs84 coordinate system
   * @return new instance of {@link OSHDBBoundingBox}
   */
  public static OSHDBBoundingBox bboxWgs84Coordinates(double minLon, double minLat, double maxLon,
      double maxLat) {
    return bboxOSMCoordinates(
        Math.toIntExact(Math.round(minLon * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(minLat * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(maxLon * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(maxLat * GEOM_PRECISION_TO_LONG)));
  }

  /**
   * Creates an {@code OSHDBBoundingBox} with wgs84 coordinates.
   *
   * @param minLon minimum longitude in wgs84 coordinate system
   * @param minLat minimum latitude in wgs84 coordinate system
   * @param maxLon maximum longitude in wgs84 coordinate system
   * @param maxLat maximum latitude in wgs84 coordinate system
   *
   * @deprecated use {@link #bboxWgs84Coordinates(double, double, double, double)
   *             bboxWgs84Coordinates} instead
   */
  @Deprecated(forRemoval = true, since = "0.7")
  public OSHDBBoundingBox(double minLon, double minLat, double maxLon, double maxLat) {
    this(Math.toIntExact(Math.round(minLon * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(minLat * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(maxLon * GEOM_PRECISION_TO_LONG)),
        Math.toIntExact(Math.round(maxLat * GEOM_PRECISION_TO_LONG)));
  }

  @Override
  public int getMinLongitude() {
    return minLon;
  }

  @Override
  public int getMaxLongitude() {
    return maxLon;
  }

  @Override
  public int getMinLatitude() {
    return minLat;
  }

  @Override
  public int getMaxLatitude() {
    return maxLat;
  }

  @Override
  public String toString() {
    return String.format(Locale.ENGLISH,
        "(%3.7f,%3.7f,%3.7f,%3.7f)",
        OSMCoordinates.toWgs84(minLon),
        OSMCoordinates.toWgs84(minLat),
        OSMCoordinates.toWgs84(maxLon),
        OSMCoordinates.toWgs84(maxLat));
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
