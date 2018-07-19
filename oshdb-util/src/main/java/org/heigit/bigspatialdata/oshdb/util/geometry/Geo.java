package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

/**
 * Geometry utility functions
 */
public class Geo {

	// todo what radius is this? At equator it should be 6378 km
	public static double earthRadius = 6371000.; //meters
	public static double ONE_DEGREE_IN_METERS_AT_EQUATOR = earthRadius * Math.PI/180.;

	// =====================
	// = line calculations =
	// =====================

	public static double distanceBetweenCoordinatesHaversine(double lat1, double lng1, double lat2, double lng2) {
		double dLat = Math.toRadians(lat2-lat1);
		double dLng = Math.toRadians(lng2-lng1);
		double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
			Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng/2) * Math.sin(dLng/2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

		return earthRadius * c;
	}

	// Equirectangular distance approximation (works well assuming segments are short)
	public static double distanceBetweenCoordinates(double lat1, double lng1, double lat2, double lng2) {
		double dLat = Math.toRadians(lat2-lat1);
		double dLng = Math.toRadians(lng2-lng1);
		dLng *= Math.cos(Math.toRadians((lat2+lat1)/2));

		return earthRadius * Math.sqrt(dLng*dLng + dLat*dLat);
	}

	public static double convertMetricDistanceToDegreeLongitude(double latitude, double distanceInMeters) {
		return distanceInMeters / (Math.cos(Math.toRadians(latitude)) * ONE_DEGREE_IN_METERS_AT_EQUATOR);
	}

	public static double lengthOf(LineString line) {
		double dist = 0.0;
		Coordinate[] coords = line.getCoordinates();

		for (int i = 1; i < coords.length; i++) {
			dist += distanceBetweenCoordinates(
				coords[i-1].y, coords[i-1].x,
				coords[i].y,   coords[i].x
			);
		}

		return dist;
	}

	public static double lengthOf(MultiLineString multiline) {
		double dist = 0.0;
		for (int i=0; i<multiline.getNumGeometries(); i++)
			dist += lengthOf((LineString) multiline.getGeometryN(i));
		return dist;
	}

	public static double lengthOf(Geometry geom) {
		if (geom instanceof LineString)
			return lengthOf((LineString)geom);
		if (geom instanceof MultiLineString)
			return lengthOf((MultiLineString)geom);
		return 0.0;
	}

	// =====================
	// = area calculations =
	// =====================

	public static double areaOf(Polygon poly) {
		double area = 0.0;
		area += Math.abs(ringArea((LinearRing)(poly.getExteriorRing())));
		for (int i = 1; i < poly.getNumInteriorRing(); i++) {
			area -= Math.abs(ringArea((LinearRing)(poly.getInteriorRingN(i))));
		}

		return area;
	}

	public static double areaOf(MultiPolygon multipoly) {
		double area = 0.0;
		for (int i=0; i<multipoly.getNumGeometries(); i++)
			area += areaOf((Polygon) multipoly.getGeometryN(i));
		return area;
	}

	public static double areaOf(Geometry geom) {
		if (geom instanceof Polygon)
			return areaOf((Polygon)geom);
		if (geom instanceof MultiPolygon)
			return areaOf((MultiPolygon)geom);
		return 0.0;
	}

	/**
	 * Calculate the approximate area of the polygon were it projected onto
	 *     the earth.  Note that this area will be positive if ring is oriented
	 *     clockwise, otherwise it will be negative.
	 *
	 * Ported to Java from https://github.com/mapbox/geojson-area/
	 *
	 * Reference:
	 * Robert. G. Chamberlain and William H. Duquette, "Some Algorithms for
	 *     Polygons on a Sphere", JPL Publication 07-03, Jet Propulsion
	 *     Laboratory, Pasadena, CA, June 2007 http://trs-new.jpl.nasa.gov/dspace/handle/2014/40409
	 *
	 * Returns:
	 * {float} The approximate signed geodesic area of the polygon in square meters.
	 */
	public static double ringArea(LinearRing ring) {
		double area = 0.0;
		Coordinate[] coords = ring.getCoordinates();
		int coordsLength = coords.length;
		int i, lowerIndex, middleIndex, upperIndex;
		Coordinate p1,p2,p3;

		if (coordsLength > 2) {
			for (i = 0; i < coordsLength; i++) {
				if (i == coordsLength - 2) {// i = N-2
					lowerIndex = coordsLength - 2;
					middleIndex = coordsLength -1;
					upperIndex = 0;
				} else if (i == coordsLength - 1) {// i = N-1
					lowerIndex = coordsLength - 1;
					middleIndex = 0;
					upperIndex = 1;
				} else { // i = 0 to N-3
					lowerIndex = i;
					middleIndex = i+1;
					upperIndex = i+2;
				}
				p1 = coords[lowerIndex];
				p2 = coords[middleIndex];
				p3 = coords[upperIndex];
				area += (Math.toRadians(p3.x) - Math.toRadians(p1.x)) * Math.sin(Math.toRadians(p2.y));
			}

			area = area * earthRadius * earthRadius / 2;
		}

		return area;
	}

	// =====================
	// = geometry clipping =
	// =====================

	public static Geometry clip(Geometry obj, OSHDBBoundingBox bbox) {
		Envelope envelope = new Envelope(bbox.getMinLon(), bbox.getMaxLon(), bbox.getMinLat(), bbox.getMaxLat());
		return obj.intersection(JTS.toGeometry(envelope));
	}

	public static <P extends Geometry & Polygonal> Geometry clip(Geometry obj, P poly) {
		return obj.intersection(poly);
	}

	// ======================
	// = geometry relations =
	// ======================

	/**
	 * Checks whether a geometry lies within a distance from another geometry
	 * The algorithm searches for the two closest points and calcualtes the distance between them.
	 * @param geom1 Geometry 1
	 * @param geom2 Geometry 2
	 * @param distanceInMeter distance in meters within which that two geometries should lie
	 * @return True, if the geometry is within the distance of the other geometry, otherwise false
	 */
	//todo: check what is returned, if one polygon contains the other. It should be 0 but it will
	// probabily return something else
	public static boolean isWithinDistance(Geometry geom1, Geometry geom2, double distanceInMeter) {
		// Find nearest points of the geometries
		Coordinate[] nearestPoints = DistanceOp.nearestPoints(geom1, geom2);
		// Calculate distance between nearest points in meters
		double dist = Geo.distanceBetweenCoordinatesHaversine(nearestPoints[0].y, nearestPoints[0].x, nearestPoints[1].y, nearestPoints[1].x);
		return dist <= distanceInMeter;
	}

}
