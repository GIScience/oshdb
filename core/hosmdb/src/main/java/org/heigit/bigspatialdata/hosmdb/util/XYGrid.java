package org.heigit.bigspatialdata.hosmdb.util;

import java.util.SortedSet;
import java.util.TreeSet;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

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
 * Longitude +180 will be wrapped around to -180. Coordinates lying on
 * grid-borders will be assigned to the north-eastern cell.
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 *
 */
public class XYGrid {

  private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger(XYGrid.class.getName());

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
      LOG.warning(
              "Zoom is too big, maximum number of tiles exeeds biggest possible Long. The maximum zoom of 30 is used instead.");
      this.zoom = 30;
    } else if (zoom < 0) {
      LOG.warning("Zoom is too small. The minimum zoom of 0 (equals 1 tile) is used instead.");
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
   * Calculates BBOX of given Cell.
   *
   * @param cellId ID of a cell calculated by getID
   * @return a BBOX for that cell (minlong, maxlong; minlat, maxlat)
   */
  public MultiDimensionalNumericData getCellDimensions(final long cellId) {

    //calculate the row and column, this tile-value corresponds to
    int x = (int) (cellId % zoompow);
    int y = (int) ((cellId - x) / zoompow);
    //calculate the values of the south-western most corner
    double lon = (x * cellWidth) - 180.0;
    double lat = (y * cellWidth) - 90.0;

    final NumericRange longitude;
    if (equalsEpsilon(lon, -180.0)) {
      longitude = new NumericRange(180.0, (lon + cellWidth) - EPSILON);
    } else {
      longitude = new NumericRange(lon, (lon + cellWidth) - EPSILON);
    }

    final NumericRange latitude;
    if (zoom == 0) {
      latitude = new NumericRange(-90.0, 90.0);
    } else if (equalsEpsilon(lat, 90.0 - cellWidth)) {
      latitude = new NumericRange(lat, 90.0);
    } else {
      latitude = new NumericRange(lat, (lat + cellWidth) - EPSILON);
    }

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

  /**
   * Returns number of Cells within given BBOX.
   *
   * @param data BBOX to estimate number of cells for
   * @return estimated number of Cells
   */
  public long getEstimatedIdCount(final MultiDimensionalNumericData data) {
    //asserts fails if estimation fault exeeds zoom
    final double[] mins = data.getMinValuesPerDimension();
    final double[] maxes = data.getMaxValuesPerDimension();
    //number of Cells in x * number of cells in y
    return (long) ((maxes[0] - mins[0]) / cellWidth * (maxes[1] - mins[1]) / cellWidth);
  }

  /**
   * getter for zoomlevel.
   *
   * @return zoomlevel
   */
  public int getLevel() {
    return zoom;
  }

  /**
   * Calculates all tiles, that lie within a bounding-box.
   *
   * @param bbox The bounding box.
   * @param enlarge if true, the BBOX is enlarged by one tile to the south-west
   * to include tiles that possibly hold way or relation information, if false
   * only holds tiles that intersect with the given BBOX. For queries: false is
   * for nodes while true is for ways and relations.
   *
   * @return Returns a set of Tile-IDs that exactly lie within the given BBOX.
   */
  public SortedSet<Long> bbox2Ids(BoundingBox bbox, boolean enlarge) {
    MultiDimensionalNumericData mdBbox = new BasicNumericDataset(new NumericData[]{new NumericRange(bbox.minLon, bbox.maxLon), new NumericRange(bbox.minLat, bbox.maxLat)});
    return this.bbox2Ids(mdBbox, enlarge);
  }

  /**
   * Calculates all tiles, that lie within a bounding-box. TODO but priority
   * 999: Add possibility to snap the BBX to the tile-grid. TODO: is an
   * exception needed?
   *
   * @param bbox The bounding box. First dimension is longitude, second is latitude.
   * @param enlarge if true, the BBOX is enlarged by one tile to the south-west
   * to include tiles that possibly hold way or relation information, if false
   * only holds tiles that intersect with the given BBOX. For queries: false is
   * for nodes while true is for ways and relations.
   *
   * @return Returns a set of Tile-IDs that lie within the given BBOX.
   */
  public SortedSet<Long> bbox2Ids(MultiDimensionalNumericData bbox, boolean enlarge) {
    //initalise basic variables
    TreeSet<Long> result = new TreeSet<>();
    double minlong = bbox.getMinValuesPerDimension()[0];
    double minlat = bbox.getMinValuesPerDimension()[1];
    double maxlong = bbox.getMaxValuesPerDimension()[0];
    double maxlat = bbox.getMaxValuesPerDimension()[1];

    if (minlat > maxlat) {
      LOG.warning("The minimum values are not smaller than the maximum values. This might throw an exeption one day?");
      return null;
    }

    //test if bbx is on earth or extends further
    if (minlong < -180.0 || minlong > 180.0) {
      result.add(-1L);
      minlong = -180.0;
    }
    if (minlat < -90.0 || minlat > 90.0) {
      result.add(-1L);
      minlat = -90.0;
    }
    if (maxlong > 180.0 || maxlong < -180.0) {
      result.add(-1L);
      maxlong = 180.0;
    }
    if (maxlat > 90.0 || maxlat < -90.0) {
      result.add(-1L);
      maxlat = 90.0;
    }

    if (equalsEpsilon(minlong, 180.0)) {
      minlong = -180.0;
    }
    if (equalsEpsilon(maxlat, 90.0)) {
      maxlat -= EPSILON;
    }
    if (equalsEpsilon(minlat, 90.0)) {
      minlat -= EPSILON;
    }
    if (equalsEpsilon(maxlong, 180.0)) {
      maxlong = -180.0;
    }

    //cope with BBOX extending over the date-line
    if (minlong > maxlong) {
      NumericRange longitude = new NumericRange(minlong, 180.0 - EPSILON);
      NumericRange latitude = new NumericRange(minlat, maxlat);
      NumericData[] dataPerDimension = new NumericData[]{longitude, latitude};
      result.addAll(bbox2Ids(new BasicNumericDataset(dataPerDimension), enlarge));

      minlong = -180.0;
    }

    //At this point the following should be true
    //minlong[-180.0:179.999999]<=maxlong[-180.0:179.9999]
    //minlat[0:89.99999999999]<=maxlat[0:89.99999999999]
    //
    //refuse to calculate results, that would retun an object > 100mb
    if (getEstimatedIdCount(bbox) > 104857600 / 4) {
      LOG.warning("The resulting collection would be bigger than 100mb. I refuse to calculate it! Think of something else e.g. a smaller area!");
      return null;
    }

    int columnmin;
    int columnmax;
    int rowmin;
    int rowmax;

    if (enlarge) {
      //calculate column and row range
      columnmin = (int) ((minlong + 180.0) / cellWidth);
      columnmax = (int) ((maxlong + 180.0) / cellWidth);
      rowmin = (int) ((minlat + 90.0) / cellWidth);
      rowmax = (int) ((maxlat + 90.0) / cellWidth);
      //it is impossible vor features to span over the datelimit, so the enlargement stops at column 0
      if (columnmin > 0) {
        columnmin -= 1;
      }
      if (rowmin > 0) {
        rowmin -= 1;
      }
    } else {
      //calculate column and row range
      columnmin = (int) ((minlong + 180.0) / cellWidth);
      columnmax = (int) ((maxlong + 180.0) / cellWidth);
      rowmin = (int) ((minlat + 90.0) / cellWidth);
      rowmax = (int) ((maxlat + 90.0) / cellWidth);
    }

    //add the regular values
    for (int r = rowmin; r <= rowmax; r++) {
      for (int c = columnmin; c <= columnmax; c++) {
        result.add(r * zoompow + c);
      }
    }
    return result;
  }

}
