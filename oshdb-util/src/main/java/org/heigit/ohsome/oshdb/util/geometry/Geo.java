package org.heigit.ohsome.oshdb.util.geometry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;

/**
 * Geometry utility functions.
 */
public class Geo {
  private static final double earthRadiusMean = 6371000.0; //meters
  private static final double earthRadiusEquator = 6378137.0; //meters
  private static final double earthInverseFlattening = 298.257223563;
  private static final double f_ = 1.0 - 1.0 / earthInverseFlattening;
  // this partially accounts for the non-spherical shape of the earth
  // see https://gis.stackexchange.com/a/63047/41632
  private static final double sphereFact = Math.pow(f_, 1.5);

  private Geo() {
    throw new IllegalStateException("Utility class");
  }

  // =====================
  // = line calculations =
  // =====================

  /**
   * Calculate the approximate length of a line string.
   *
   * <p>
   * Uses an equirectangular distance approximation, which works well assuming segments are short.
   * </p>
   *
   * <p>
   * Adjusted to partially account for the spheroidal shape of the earth (WGS84 coordinates).
   * See https://gis.stackexchange.com/a/63047/41632
   * </p>
   *
   * <p>
   * For typical features present in OpenStreetMap data, the relative error introduced by
   * this approximation is below 0.1%
   * </p>
   *
   * @param line the coordinates of the line. coordinates must be in WGS84
   * @return The approximate geodesic length of the line string in meters.
   */
  public static double lengthOf(LineString line) {
    return lengthOf(line.getCoordinates());
  }

  private static double lengthOf(Coordinate[] coords) {
    double dist = 0.0;

    if (coords.length > 1) {
      double prevLon = Math.toRadians(coords[0].x);
      double prevLat = Math.atan(sphereFact * Math.tan(Math.toRadians(coords[0].y)));
      for (int i = 1; i < coords.length; i++) {
        double thisLon = Math.toRadians(coords[i].x);
        double thisLat = Math.atan(sphereFact * Math.tan(Math.toRadians(coords[i].y)));
        double deltaLon = thisLon - prevLon;
        double deltaLat = thisLat - prevLat;
        deltaLon *= Math.cos((thisLat + prevLat) / 2);
        dist += Math.sqrt(deltaLon * deltaLon + deltaLat * deltaLat);
        prevLon = thisLon;
        prevLat = thisLat;
      }
      dist *= earthRadiusMean;
    }

    return dist;
  }

  /**
   * Calculate the approximate length of a multi line string.
   *
   * <p>See {@link #lengthOf(LineString)} for further details.</p>
   *
   * @param multiline the geometry of the multi line. coordinates must be in WGS84
   * @return The approximate geodesic length of the line string in meters.
   */
  public static double lengthOf(MultiLineString multiline) {
    double dist = 0.0;
    for (int i = 0; i < multiline.getNumGeometries(); i++) {
      dist += lengthOf((LineString) multiline.getGeometryN(i));
    }
    return dist;
  }

  /**
   * Calculate the approximate length of a multi geometry.
   *
   * <p>See {@link #lengthOf(LineString)} for further details.</p>
   *
   * @param geometryCollection the geometries to get the length of. coordinates must be in WGS84
   * @return The approximate geodesic length of the linear geometries of this collection in meters.
   */
  public static double lengthOf(GeometryCollection geometryCollection) {
    double dist = 0.0;
    for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
      dist += lengthOf(geometryCollection.getGeometryN(i));
    }
    return dist;
  }

  /**
   * Calculate the approximate length of an arbitrary geometry.
   *
   * <p>Returns zero for non-linear features such as points or polygons.</p>
   *
   * <p>See {@link #lengthOf(LineString)} for further details.</p>
   *
   * @param geom the geometry to get the length of. coordinates must be in WGS84
   * @return The approximate geodesic length of the geometry in meters.
   */
  public static double lengthOf(Geometry geom) {
    if (geom instanceof LineString) {
      return lengthOf((LineString) geom);
    }
    if (geom instanceof MultiLineString) {
      return lengthOf((MultiLineString) geom);
    }
    if (geom instanceof GeometryCollection) {
      return lengthOf((GeometryCollection) geom);
    }
    return 0.0;
  }

  // =====================
  // = area calculations =
  // =====================


  /**
   * Calculate the approximate area of an arbitrary geometry.
   *
   * <p>
   * This uses approximation formulas for the area of small polygons on a sphere, but partially
   * accounts for the spheroidal shape of the earth (geometry coordinates given in WGS84).
   * </p>
   *
   * <p>
   * For typical features present in OpenStreetMap data, the relative error introduced by
   * this approximation is below 0.1%
   * </p>
   *
   * <p>
   * Reference:
   *   Robert. G. Chamberlain and William H. Duquette, "Some Algorithms for
   *   Polygons on a Sphere", JPL Publication 07-03, Jet Propulsion
   *   Laboratory, Pasadena, CA, June 2007
   *   https://trs.jpl.nasa.gov/handle/2014/40409
   * </p>
   *
   * <p>
   * This method will never return a negative number. For invalid polygons with a larger inner
   * rings area than the outer ring encompasses, zero is returned instead.
   * </p>
   *
   * @param poly the polygon for which the area should be calculated. coordinates must be in WGS84
   * @return The approximate signed geodesic area of the polygon in square meters.
   */
  public static double areaOf(Polygon poly) {
    double area = 0.0;
    area += Math.abs(ringArea((LinearRing) poly.getExteriorRing()));
    for (int i = 0; i < poly.getNumInteriorRing(); i++) {
      area -= Math.abs(ringArea((LinearRing) poly.getInteriorRingN(i)));
    }
    return Math.max(0, area);
  }

  /**
   * Calculate the approximate area of a multi polygon.
   *
   * <p>See {@link #areaOf(Polygon)} for further details.</p>
   *
   * @param multipoly the multipolygon for which the area should be calculated.
   *                  coordinates must be in WGS84
   * @return The approximate signed geodesic area of the multipolygon in square meters.
   */
  public static double areaOf(MultiPolygon multipoly) {
    double area = 0.0;
    for (int i = 0; i < multipoly.getNumGeometries(); i++) {
      area += areaOf((Polygon) multipoly.getGeometryN(i));
    }
    return area;
  }


  /**
   * Calculate the approximate area of a geometry collection.
   *
   * <p>See {@link #areaOf(Polygon)} for further details.</p>
   *
   * @param geometryCollection the geometry collection for which the area should be calculated.
   *                           coordinates must be in WGS84
   * @return The approximate signed geodesic area of the geometry collection in square meters.
   */
  public static double areaOf(GeometryCollection geometryCollection) {
    double area = 0.0;
    for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
      area += areaOf(geometryCollection.getGeometryN(i));
    }
    return area;
  }

  /**
   * Calculate the approximate area of an arbitrary geometry.
   *
   * <p>Returns zero for non-polygonal features such as points or lines.</p>
   *
   * <p>See {@link #areaOf(Polygon)} for further details.</p>
   *
   * @param geom the geometry for which the area should be calculated. coordinates must be in WGS84
   * @return The approximate signed geodesic area of the geometry in square meters.
   */
  public static double areaOf(Geometry geom) {
    if (geom instanceof Polygon) {
      return areaOf((Polygon) geom);
    }
    if (geom instanceof MultiPolygon) {
      return areaOf((MultiPolygon) geom);
    }
    if (geom instanceof GeometryCollection) {
      return areaOf((GeometryCollection) geom);
    }
    return 0.0;
  }

  /**
   * Calculate the approximate area of the polygon.
   *
   * <p>
   * Note that this area will be positive if ring is oriented clockwise,
   * otherwise it will be negative.
   * </p>
   *
   * <p>
   * Initially ported to Java from https://github.com/mapbox/geojson-area/.
   * Later adjusted to partially account for the spheroidal shape of the earth (WGS84 coordinates).
   * </p>
   *
   * <p>
   * For typical features present in OpenStreetMap data, the relative error introduced by
   * this approximation is below 0.1%
   * </p>
   *
   * <p>
   * Reference:
   *   Robert. G. Chamberlain and William H. Duquette, "Some Algorithms for
   *   Polygons on a Sphere", JPL Publication 07-03, Jet Propulsion
   *   Laboratory, Pasadena, CA, June 2007
   *   https://trs.jpl.nasa.gov/handle/2014/40409
   * </p>
   *
   * @param ring the closed ring delimiting the area to be calculated. coordinates must be in WGS84
   * @return The approximate signed geodesic area of the polygon in square meters.
   */
  private static double ringArea(LinearRing ring) {
    double area = 0.0;
    Coordinate[] coords = ring.getCoordinates();
    int coordsLength = coords.length;

    if (coordsLength > 2) {
      for (int i = 0; i < coordsLength; i++) {
        int lowerIndex;
        int middleIndex;
        int upperIndex;
        if (i == coordsLength - 2) { // i = N-2
          lowerIndex = coordsLength - 2;
          middleIndex = coordsLength - 1;
          upperIndex = 0;
        } else if (i == coordsLength - 1) { // i = N-1
          lowerIndex = coordsLength - 1;
          middleIndex = 0;
          upperIndex = 1;
        } else { // i = 0 to N-3
          lowerIndex = i;
          middleIndex = i + 1;
          upperIndex = i + 2;
        }
        Coordinate p1 = coords[lowerIndex];
        Coordinate p2 = coords[middleIndex];
        Coordinate p3 = coords[upperIndex];
        // wgs84 latitudes are not the same as spherical latitudes.
        // this converts the latitude from a wgs84 coordinate into its corresponding spherical value
        double x = f_ * Math.tan(Math.toRadians(p2.y));
        double sinLat = x / Math.sqrt(x * x + 1.0);
        area += (Math.toRadians(p3.x - p1.x)) * sinLat;
      }
      double midLat =
          (ring.getEnvelopeInternal().getMaxY() + ring.getEnvelopeInternal().getMinY()) / 2;
      area *= 0.5 * earthRadiusEquator * earthRadiusEquator
          * (1 - 1 / earthInverseFlattening * Math.pow(Math.cos(Math.toRadians(midLat)), 2));
    }

    return area;
  }

  // ======================
  // = shape calculations =
  // ======================

  /**
   * Calculates the "Polsby–Popper test" score of a polygonal geometry, representing the
   * roundness (or compactness) of the feature.
   *
   * <p>
   * If the shape is not polygonal, zero is returned. If a shape constitutes of multiple parts (a
   * MultiPolygon geometry), the result is the weighted sum of the individual parts' scores.
   * See <a href="https://en.wikipedia.org/wiki/Polsby%E2%80%93Popper_test">wikipedia</a> for info.
   * </p>
   *
   * @param geom the geometry for which to calculate the PP score of.
   * @return the compactness measure of the input shape. A score of 1 indicates maximum compactness
   *         (i.e. a circle)
   */
  public static double roundness(Geometry geom) {
    if (!(geom instanceof Polygonal)) {
      return 0;
    }
    var boundaryLength = Geo.lengthOf(geom.getBoundary());
    if (boundaryLength == 0) {
      return 0;
    }
    return 4 * Math.PI * Geo.areaOf(geom) / (boundaryLength * boundaryLength);
  }

  // ======================
  // = angle calculations =
  // ======================

  public static double bearingRadians(Coordinate from, Coordinate to) {
    var x1 = from.x * Math.PI / 180;
    var x2 = to.x * Math.PI / 180;
    var y1 = from.y * Math.PI / 180;
    var y2 = to.y * Math.PI / 180;
    var y = Math.sin(x2 - x1) * Math.cos(y2);
    var x = Math.cos(y1) * Math.sin(y2)
        - Math.sin(y1) * Math.cos(y2) * Math.cos(x2 - x1);
    return (Math.atan2(y, x) + 2 * Math.PI) % (2 * Math.PI);
  }

  /**
   * Returns a measure for the squareness (or rectilinearity) of a geometry.
   *
   * <p>Adapted from "A Rectilinearity Measurement for Polygons" by Joviša Žunić and Paul L. Rosin:
   * DOI:10.1007/3-540-47967-8_50
   * https://link.springer.com/chapter/10.1007%2F3-540-47967-8_50
   * https://www.researchgate.net/publication/221304067_A_Rectilinearity_Measurement_for_Polygons
   *
   * Žunić J., Rosin P.L. (2002) A Rectilinearity Measurement for Polygons. In: Heyden A., Sparr G.,
   * Nielsen M., Johansen P. (eds) Computer Vision — ECCV 2002. ECCV 2002. Lecture Notes in Computer
   * Science, vol 2351. Springer, Berlin, Heidelberg. https://doi.org/10.1007/3-540-47967-8_50
   * </p>
   *
   * <p>Adjusted to work directly on geographic coordinates. Implementation works on a few
   * assumptions: input geometries are "small"; spherical globe approximation.</p>
   *
   * @param geom a Polygon, MultiPolygon and LineString for which the squareness is to be evaluated.
   * @return returns the rectilinearity value of the input geometry, or zero if the geometry type
   *         isn't supported
   */
  public static double squareness(Geometry geom) {
    if (geom instanceof Polygon) {
      return squareness(dissolvePolygonToRings((Polygon) geom));
    } else if (geom instanceof MultiPolygon) {
      var multiPoly = (MultiPolygon) geom;
      var rings = new ArrayList<LinearRing>();
      for (var i = 0; i < multiPoly.getNumGeometries(); i++) {
        var poly = (Polygon) geom.getGeometryN(i);
        rings.addAll(dissolvePolygonToRings(poly));
      }
      return squareness(rings);
    } else if (geom instanceof LineString) {
      return squareness(Collections.singletonList((LineString) geom));
    } else {
      // other geometry types: return 0
      return 0;
    }
  }

  /** Helper method to dissolve a polygon to a collection of rings. */
  private static List<LinearRing> dissolvePolygonToRings(Polygon poly) {
    var rings = new ArrayList<LinearRing>(poly.getNumInteriorRing() + 1);
    rings.add(poly.getExteriorRing());
    for (var i = 0; i < poly.getNumInteriorRing(); i++) {
      rings.add(poly.getInteriorRingN(i));
    }
    return rings;
  }

  private static double squareness(List<? extends LineString> lines) {
    var minLengthL1 = Double.MAX_VALUE;
    for (LineString line : lines) {
      var coords = line.getCoordinates();
      for (var j = 1; j < coords.length; j++) {
        var angle = bearingRadians(coords[j - 1], coords[j]);
        var lengthL1 = 0.0;
        for (LineString lineAgain : lines) {
          lengthL1 += gridAlignedLengthL1(lineAgain, angle);
        }
        if (lengthL1 < minLengthL1) {
          minLengthL1 = lengthL1;
        }
      }
    }
    var lengthL2 = 0.0;
    for (LineString line : lines) {
      lengthL2 += lengthOfL2(line.getCoordinates());
    }
    if (minLengthL1 == 0) {
      return 0;
    }
    return 4 / (4 - Math.PI) * (lengthL2 / minLengthL1 - Math.PI / 4);
  }

  /**
   * Intermediate value used in calculation of squareness metric, see paper referenced above.
   * */
  private static double gridAlignedLengthL1(LineString line, double angle) {
    var cosA = Math.cos(angle);
    var sinA = Math.sin(angle);
    var centroid = line.getCentroid().getCoordinate();
    var cosCentroidY = Math.cos(centroid.y * Math.PI / 180);
    var inverseCosCentroidY = 1 / cosCentroidY;
    var coords = line.getCoordinates();
    var modifiedCoords = new Coordinate[coords.length];
    // shift to origin
    for (var i = 0; i < coords.length; i++) {
      modifiedCoords[i] = new Coordinate(
          (coords[i].x - centroid.x) * cosCentroidY,
          coords[i].y - centroid.y
      );
    }
    // rotate
    for (Coordinate modifiedCoord : modifiedCoords) {
      var newX = modifiedCoord.x * cosA - modifiedCoord.y * sinA;
      modifiedCoord.y = modifiedCoord.x * sinA + modifiedCoord.y * cosA;
      modifiedCoord.x = newX;
    }
    // shift back to original location
    for (Coordinate modifiedCoord : modifiedCoords) {
      modifiedCoord.x = modifiedCoord.x * inverseCosCentroidY + centroid.x;
      modifiedCoord.y += centroid.y;
    }
    return lengthOfL1(modifiedCoords);
  }

  /**
   * Intermediate value used in calculation of squareness metric, see paper referenced above
   * (this uses the L1 "Manhattan" metric to calculate the length of a given linestring).
   * */
  private static double lengthOfL1(Coordinate[] coords) {
    var dist = 0.0;
    if (coords.length > 1) {
      double prevLon = Math.toRadians(coords[0].x);
      double prevLat = Math.toRadians(coords[0].y);
      for (var i = 1; i < coords.length; i++) {
        double thisLon = Math.toRadians(coords[i].x);
        double thisLat = Math.toRadians(coords[i].y);
        double deltaLon = thisLon - prevLon;
        double deltaLat = thisLat - prevLat;
        deltaLon *= Math.cos((thisLat + prevLat) / 2);
        dist += Math.abs(deltaLon) + Math.abs(deltaLat);
        prevLon = thisLon;
        prevLat = thisLat;
      }
    }
    return dist;
  }

  /**
   * Intermediate value used in calculation of squareness metric, see paper referenced above
   * (this uses the L2 "euclidean" distance metric to calculate the length of a given linestring).
   * */
  private static double lengthOfL2(Coordinate[] coords) {
    var dist = 0.0;
    if (coords.length > 1) {
      double prevLon = Math.toRadians(coords[0].x);
      double prevLat = Math.toRadians(coords[0].y);
      for (var i = 1; i < coords.length; i++) {
        double thisLon = Math.toRadians(coords[i].x);
        double thisLat = Math.toRadians(coords[i].y);
        double deltaLon = thisLon - prevLon;
        double deltaLat = thisLat - prevLat;
        deltaLon *= Math.cos((thisLat + prevLat) / 2);
        dist += Math.sqrt(deltaLon * deltaLon + deltaLat * deltaLat);
        prevLon = thisLon;
        prevLat = thisLat;
      }
    }
    return dist;
  }

  // =====================
  // = geometry clipping =
  // =====================

  public static Geometry clip(Geometry obj, OSHDBBoundingBox bbox) {
    return obj.intersection(OSHDBGeometryBuilder.getGeometry(bbox));
  }

  public static <P extends Geometry & Polygonal> Geometry clip(Geometry obj, P poly) {
    return obj.intersection(poly);
  }
}
