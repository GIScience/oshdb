package org.heigit.bigspatialdata.hosmdb.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;


/**
 * XYGrid spans an equal degree grid over the world
 * 
 * 
 * for zoom = 2
 * 
 *       +90 lat 
 *          ^
 *      4 5 | 6 7 
 * -180 - - 0 - - +180 longitude 
 *      0 1 | 2 3 
 *          Y 
 *         -90 lat
 * 
 * Longitude 180 will be wrapped around to -180
 * 
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 *
 */
public class XYGrid{

  private static final Logger LOGGER = LoggerFactory.getLogger(XYGrid.class);


  private final int zoom;
  private final long zoompow;
  private final double cellWidth;


  /**
   *
   * @param zoom The zoom to be used. The number of tiles in a row equals 2^zoom. The maximum zoom
   *        is 30 due to the maximum accuracy and values of used numeric formats.
   */
  public XYGrid(final int zoom) {
    if (zoom > 30) {
      LOGGER.warn(
          "Zoom is too big, maximum number of tiles exeeds biggest possible Long. The maximum zoom of 30 is used instead.");
      this.zoom = 30;
    } else if (zoom < 0) {
      LOGGER.warn("Zoom is too small. The maximum zoom of 0 (equals 1 tile) is used instead.");
      this.zoom = 0;
    } else {
      this.zoom = zoom;
    }

    zoompow = (long) Math.pow(2, zoom);
    cellWidth = 360.0 / zoompow;
  }



  public long getId(double longitude, double latitude) {
    if (longitude > 180 || longitude < -180 || latitude > 90 || latitude < -90)
      return -1l;

    longitude += 180.0;
    latitude += 90.0;

    if (equalsEpsilon(longitude, 360.0)) {
      // wrap arround
      longitude = 0.0;
    }
    if (equalsEpsilon(latitude, 180.0)) {
      // fix latitude to clostest value under 180
      latitude -= EPSILON;
    }

    final int x = (int) (longitude / cellWidth);
    final int y = (int) (latitude / cellWidth);

    return y * zoompow + x;
  }


  /**
   * getId return the smallest Id of the given Bounding Box
   * 
   * 
   * @param longitudes
   * @param latitudes
   * @return
   */
  public long getId(final NumericRange longitudes, final NumericRange latitudes) {
    return getId(longitudes.getMin(), latitudes.getMin());
  }


  public double getCellWidth() {
    return cellWidth;
  }

  public MultiDimensionalNumericData getCellDimensions(final long cellId) {

    int x = (int) (cellId % zoompow);
    int y = (int) ((cellId - x) / zoompow);
    double lon = (x * cellWidth) - 180.0;
    double lat = (y * cellWidth) - 90.0;

    final NumericRange longitude = new NumericRange(lon, lon + cellWidth);
    final NumericRange latitude = new NumericRange(lat, lat + cellWidth);
    final NumericData[] dataPerDimension = new NumericData[] {longitude, latitude};
    return new BasicNumericDataset(dataPerDimension);
  }



  private static final double EPSILON = 1e-11;

  /**
   * Determines if the two given double values are equal (their delta being smaller than a fixed
   * epsilon)
   * 
   * @param a The first double value to compare
   * @param b The second double value to compare
   * @return {@code true} if {@code abs(a - b) <= 1e-11}, {@code false} otherwise
   */
  public static boolean equalsEpsilon(double a, double b) {
    return Math.abs(a - b) <= EPSILON;
  }


  public long getEstimatedIdCount(final MultiDimensionalNumericData data) {
    final double[] mins = data.getMinValuesPerDimension();
    final double[] maxes = data.getMaxValuesPerDimension();
    return (long)((maxes[0] - mins[0])/cellWidth * (maxes[1]-mins[1])/cellWidth);
  }


  public int getLevel() {
    return zoom;
  }

}
