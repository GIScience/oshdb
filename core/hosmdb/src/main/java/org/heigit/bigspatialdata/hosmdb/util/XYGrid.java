package org.heigit.bigspatialdata.hosmdb.util;

import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XYGrid spans an equal degree grid over the world.
 * <br>
 * <br>
 * IDs for zoom = 2:
 * <table summary="how XYGrid sees the world" style="text-align:center; border-spacing: 4px">
 * <tr>
 * <td></td><td></td><td colspan="3">+90 lat</td><td></td><td></td><td></td>
 * </tr>
 * <tr>
 * <td></td><td>4</td><td>5</td><td>|</td><td>6</td><td>7</td><td></td><td></td>
 * </tr>
 * <tr>
 * <td>-180</td><td>-</td><td>-</td><td>0</td><td>-</td><td>-</td><td>+180</td><td>longitude</td>
 * </tr>
 * <tr>
 * <td></td><td>0</td><td>1</td><td>|</td><td>2</td><td>3</td><td></td><td></td>
 * </tr>
 * <tr>
 * <td></td><td></td><td colspan="3">-90 lat</td><td></td><td></td><td></td>
 * </tr>
 * </table>
 * <br>
 * Longitude 180 will be wrapped around to -180
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 *
 */
public class XYGrid {

  private static final Logger LOGGER = LoggerFactory.getLogger(XYGrid.class);

  private final int zoom;
  private final long zoompow;
  private final double cellWidth;

  /**
   *
   * @param zoom The zoom to be used. The number of tiles in a row equals
   * 2^zoom. The maximum zoom is 30 due to the maximum accuracy and values of
   * used numeric formats.
   */
  public XYGrid(final int zoom) {
    if (zoom > 30) {
      LOGGER.warn(
              "Zoom is too big, maximum number of tiles exeeds biggest possible Long. The maximum zoom of 30 is used instead.");
      this.zoom = 30;
    } else if (zoom < 0) {
      LOGGER.warn("Zoom is too small. The minimum zoom of 0 (equals 1 tile) is used instead.");
      this.zoom = 0;
    } else {
      this.zoom = zoom;
    }

    zoompow = (long) Math.pow(2, zoom);
    cellWidth = 360.0 / zoompow;
  }

  /**
   * Returns the covering tile of a coordinate. Coordinates lying on borders
   * will be placed in the northern and/or eastern tile except for coordinates
   * on the North Pole which are placed in the northernmost tile.
   *
   * @param longitude Longitude of the point
   * @param latitude Latitude of the point
   * @return Returns the ID of the tile as shown above
   */
  public long getId(double longitude, double latitude) {
    //return -1, if point is outside geographical coordinate range
    if (longitude > 180 || longitude < -180 || latitude > 90 || latitude < -90) {
      return -1l;
    }

    longitude += 180.0;
    latitude += 90.0;

    //if it reaches the eastern most border,it is placed on the western most tile
    if (equalsEpsilon(longitude, 360.0)) {
      // wrap arround
      longitude = 0.0;
    }
    //if it reaches the northpole, it is placed in the northern most tile
    if (equalsEpsilon(latitude, 180.0)) {
      // fix latitude to clostest value under 180
      latitude -= EPSILON;
    }

    //calculate column and row
    final int x = (int) (longitude / cellWidth);
    final int y = (int) (latitude / cellWidth);

    return y * zoompow + x;
  }

  /**
   * Returns the smallest Id of the given Bounding Box.
   *
   *
   * @param longitudes longitude-range
   * @param latitudes latitude range
   * @return south-western cellID of given BBOX
   */
  public long getId(final NumericRange longitudes, final NumericRange latitudes) {
    return getId(longitudes.getMin(), latitudes.getMin());
  }

  /**
   * Returns width (and height) of cells in given Grid.
   *
   * @return length in degree of borders of cells
   */
  public double getCellWidth() {
    return cellWidth;
  }

  /**
   * Calculates BBOX of given Cell
   *
   * @param cellId ID of a cell calculated by getID
   * @return a BBOX for that cell
   */
  public MultiDimensionalNumericData getCellDimensions(final long cellId) {

    //calculate the row and column, this tile-value corresponds to
    int x = (int) (cellId % zoompow);
    int y = (int) ((cellId - x) / zoompow);
    //calculate the values of the south-western most corner
    double lon = (x * cellWidth) - 180.0;
    double lat = (y * cellWidth) - 90.0;

    final NumericRange longitude = new NumericRange(lon, lon + cellWidth);
    final NumericRange latitude = new NumericRange(lat, lat + cellWidth);
    final NumericData[] dataPerDimension = new NumericData[]{longitude, latitude};
    return new BasicNumericDataset(dataPerDimension);
  }

  private static final double EPSILON = 1e-11;

  /**
   * Determines if the two given double values are equal (their delta being
   * smaller than a fixed epsilon).
   *
   * @param a The first double value to compare
   * @param b The second double value to compare
   * @return {@code true} if {@code abs(a - b) <= 1e-11}, {@code false}
   * otherwise
   */
  public static boolean equalsEpsilon(double a, double b) {
    return Math.abs(a - b) <= EPSILON;
  }

  /**Returns number of Cells within given BBOX.
   *
   * @param data BBOX to estimate number of cells for
   * @return estimated number of Cells
   */
  public long getEstimatedIdCount(final MultiDimensionalNumericData data) {
    final double[] mins = data.getMinValuesPerDimension();
    final double[] maxes = data.getMaxValuesPerDimension();
    //number of Cells in x * number of cells in y
    return (long) ((maxes[0] - mins[0]) / cellWidth * (maxes[1] - mins[1]) / cellWidth);
  }

  /** getter for zoomlevel.
   *
   * @return zoomlevel
   */
  public int getLevel() {
    return zoom;
  }

}
