package org.heigit.bigspatialdata.hosmdb.util;

/**
 * Geometry utility functions
 */
public class Geo {

	public static double distanceBetweenInM(double lat1, double lng1, double lat2, double lng2) {
		// todo: replace with simpler approximation (assuming segments are typically nearby)
		double earthRadius = 6371000; //meters
		double dLat = Math.toRadians(lat2-lat1);
		double dLng = Math.toRadians(lng2-lng1);
		double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
			Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng/2) * Math.sin(dLng/2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));

		return earthRadius * c;
	}

}
