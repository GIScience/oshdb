package org.heigit.bigspatialdata.oshdb.index;

import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
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
 * Longitude +180 will be wrapped around to -180. Coordinates lying on
 * grid-borders will be assigned to the north-eastern cell.
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 *
 */
public class XYGrid {

  private static final Logger LOG = LoggerFactory.getLogger(XYGrid.class);

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
      LOG.warn("Zoom is too big, maximum number of tiles exceeds biggest possible Long. The maximum zoom of 30 is used instead.");
      this.zoom = 30;
    } else if (zoom < 0) {
      LOG.warn("Zoom is too small. The minimum zoom of 0 (equals 1 tile) is used instead.");
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
   * @param bbx
   * @return south-western cellID of given BBOX
   */
  public long getId(BoundingBox bbx) {
    return getId(bbx.minLon, bbx.minLat);
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
   * @return a BBOX for that cell
   */
  public BoundingBox getCellDimensions(final long cellId) {

    //calculate the row and column, this tile-value corresponds to
    int x = (int) (cellId % zoompow);
    int y = (int) ((cellId - x) / zoompow);
    //calculate the values of the south-western most corner
    double lon = (x * cellWidth) - 180.0;
    double lat = (y * cellWidth) - 90.0;

    final double minlong = lon;
    final double maxlong = (lon + cellWidth) - EPSILON;

    final double minlat;
    final double maxlat;
    if (zoom == 0) {
      minlat = -90.0;
      maxlat = 90.0;
    } else if (equalsEpsilon(lat, 90.0 - cellWidth)) {
      minlat = lat;
      maxlat = 90.0;
    } else {
      minlat = lat;
      maxlat = (lat + cellWidth) - EPSILON;
    }
    return new BoundingBox(minlong, maxlong, minlat, maxlat);
  }

  /**
   * Calculate the BoundingBox of a specific GridCell.
   *
   * @param cellID
   * @return
   */
  public static BoundingBox getBoundingBox(final CellId cellID) {
    XYGrid temp = new XYGrid(cellID.getZoomLevel());
    return temp.getCellDimensions(cellID.getId());
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
   * @return the long
   */
  public long getEstimatedIdCount(final BoundingBox data) {
    //number of Cells in x * number of cells in y
    return (long) ((data.maxLon - data.minLon) / cellWidth * (data.maxLat - data.minLat) / cellWidth);
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
   * Calculates all tiles, that lie within a bounding-box. TODO but priority
   * 999: Add possibility to snap the BBX to the tile-grid. TODO: is an
   * exception needed?
   *
   * @param bbox The bounding box. First dimension is longitude, second is
   * latitude.
   * @param enlarge if true, the BBOX is enlarged by one tile to the south-west
   * to include tiles that possibly hold way or relation information, if false
   * only holds tiles that intersect with the given BBOX. For queries: false is
   * for nodes while true is for ways and relations.
   *
   * @return Returns a set of Tile-IDs that lie within the given BBOX.
   */
  public Set<Pair<Long, Long>> bbox2CellIdRanges(BoundingBox bbox, boolean enlarge) {
    //initialise basic variables
    Set<Pair<Long, Long>> result = new TreeSet<>();
    double minlong = bbox.minLon;
    double minlat = bbox.minLat;
    double maxlong = bbox.maxLon;
    double maxlat = bbox.maxLat;

    if (minlat > maxlat) {
      LOG.warn("The minimum values are not smaller than the maximum values. This might throw an exeption one day?");
      return null;
    }

    Pair<Long, Long> outofboundsCell = new ImmutablePair<>(-1L, -1L);
    //test if bbox is on earth or extends further
    if (minlong < -180.0 || minlong > 180.0) {
      result.add(outofboundsCell);
      minlong = -180.0;
    }
    if (minlat < -90.0 || minlat > 90.0) {
      result.add(outofboundsCell);
      minlat = -90.0;
    }
    if (maxlong > 180.0 || maxlong < -180.0) {
      result.add(outofboundsCell);
      maxlong = 180.0;
    }
    if (maxlat > 90.0 || maxlat < -90.0) {
      result.add(outofboundsCell);
      maxlat = 90.0;
    }

    if (equalsEpsilon(minlong, 180.0)) {
      minlong = 180.0 - EPSILON;
    }
    if (equalsEpsilon(maxlong, 180.0)) {
      maxlong = 180.0 - EPSILON;
    }
    if (equalsEpsilon(minlat, 90.0)) {
      minlat = 90.0 - EPSILON;
    }
    if (equalsEpsilon(maxlat, 90.0)) {
      maxlat = 90.0 - EPSILON;
    }

    //cope with BBOX extending over the date-line
    if (minlong > maxlong) {
      result.addAll(bbox2CellIdRanges(new BoundingBox(minlong, 180.0 - EPSILON, minlat, maxlat), enlarge));

      minlong = -180.0;
    }

    //At this point the following should be true
    //minlong[-180.0:179.999999]<=maxlong[-180.0:179.9999]
    //minlat[0:89.99999999999]<=maxlat[0:89.99999999999]
    //calculate column and row range
    int columnmin = (int) ((minlong + 180.0) / cellWidth);
    int columnmax = (int) ((maxlong + 180.0) / cellWidth);
    int rowmin = (int) ((minlat + 90.0) / cellWidth);
    int rowmax = (int) ((maxlat + 90.0) / cellWidth);

    if (enlarge) {
      //it is impossible vor features to span over the datelimit, so the enlargement stops at column 0
      if (columnmin > 0) {
        columnmin -= 1;
      }
      if (rowmin > 0) {
        rowmin -= 1;
      }
    }
    //add the regular cell ranges
    for (int row = rowmin; row <= rowmax; row++) {
      result.add(new ImmutablePair<>(row * zoompow + columnmin, row * zoompow + columnmax));
    }
    return result;
  }

  /**
   * Get 2D neighbours of given cellId.
   *
   * @param center
   * @return a set of ID-ranges containing the neighbours of the given cell and
   * the cell itself. -1L,-1L will be added, if this cell lies on the edge of
   * the XYGrid
   */
  public Set<Pair<Long, Long>> getNeighbours(CellId center) {
    if (center.getZoomLevel() != this.zoom) {
      //might return neighbours in current zoomlevel given the bbox of the provided CellId one day
      return null;
    }

    BoundingBox bbox = this.getCellDimensions(center.getId());
    double minlong = bbox.minLon - EPSILON;
    double minlat = bbox.minLat - EPSILON;
    double maxlong = bbox.maxLon + EPSILON;
    double maxlat = bbox.maxLat + EPSILON;
    BoundingBox newbbox = new BoundingBox(minlong, maxlong, minlat, maxlat);

    return this.bbox2CellIdRanges(newbbox, false);
  }

}
