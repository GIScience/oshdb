package org.heigit.bigspatialdata.oshdb.util;

import java.util.Locale;

import org.geotools.geometry.jts.JTS;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class BoundingBox {

  public final double minLon;
  public final double maxLon;
  public final double minLat;
  public final double maxLat;

  public BoundingBox(double minLon, double maxLon, double minLat, double maxLat) {
    this.minLon = minLon;
    this.maxLon = maxLon;
    this.minLat = minLat;
    this.maxLat = maxLat;
  }

  @Override
  public String toString() {

    return String.format(Locale.ENGLISH,"(%f,%f) (%f,%f)", minLon, minLat, maxLon, maxLat);
  }
  
  /**
   * LLMA: added to provide geometry for JTS intersecion test
   * @return com.vividsolutions.jts.geom.Geometry
   */
  public Geometry toJTSGeometry() {
	return JTS.toGeometry(new Envelope(minLon, maxLon, minLat, maxLon));
}

}
