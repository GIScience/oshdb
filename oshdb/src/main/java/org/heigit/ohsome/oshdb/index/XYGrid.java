package org.heigit.ohsome.oshdb.index;

import static org.heigit.ohsome.oshdb.OSHDB.coordinateToLong;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XYGrid spans an equal degree grid over the world.
 * 
 * <p>Example IDs for zoom = 2:
 * <table style="text-align:center; border-spacing: 4px">
 * <caption>How XYGrid sees the world.</caption>
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
 *
 * <p>Longitude +180 will be wrapped around to -180. Coordinates lying on
 * grid-borders will be assigned to the north-eastern cell.</p>
 */
public class XYGrid implements Serializable {

  
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(XYGrid.class);

  /**
   * Calculate the OSHDBBoundingBox of a specific GridCell.
   *
   * @param cellID
   * @return
   */
  public static OSHDBBoundingBox getBoundingBox(final CellId cellID) {
    XYGrid grid = new XYGrid(cellID.getZoomLevel());
    return grid.getCellDimensions(cellID.getId());
  }

  /**
   * Calculate the OSHDBBoundingBox of a specific GridCell.
   *
   * @param cellID
   * @param enlarge
   * @return
   */
  public static OSHDBBoundingBox getBoundingBox(final CellId cellID, boolean enlarge) {
    if (!enlarge) {
      return getBoundingBox(cellID);
    }
    XYGrid grid = new XYGrid(cellID.getZoomLevel());
    long id = cellID.getId();
    int x = (int) (id % grid.zoompow);
    int y = (int) ((id - x) / grid.zoompow);
    long topRightId = id;
    if (x < grid.zoompow - 1) {
      topRightId += 1;
    }
    if (y < grid.zoompow / 2 - 1) {
      topRightId += grid.zoompow;
    }
    OSHDBBoundingBox result = grid.getCellDimensions(id);
    result.add(grid.getCellDimensions(topRightId));
    return result;
  }

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

    zoompow = (long) Math.pow(2, this.zoom);
    cellWidth = (360.0 / zoompow) * OSHDB.GEOM_PRECISION_TO_LONG;
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
    return this.getId(coordinateToLong(longitude), coordinateToLong(latitude));
  }

  public long getId(long longitude, long latitude) {
    //return -1, if point is outside geographical coordinate range
    if (longitude > coordinateToLong(180.0) || longitude < coordinateToLong(-180.0) || latitude > coordinateToLong(90.0) || latitude < coordinateToLong(-90.0)) {
      return -1l;
    }

    longitude += coordinateToLong(180.0);
    latitude += coordinateToLong(90.0);

    //if it reaches the eastern most border,it is placed on the western most tile
    if (longitude == coordinateToLong(360.0)) {
      // wrap arround
      longitude = 0L;
    }
    //if it reaches the northpole, it is placed in the northern most tile
    if (latitude == coordinateToLong(180.0)) {
      // fix latitude to clostest value under 180
      latitude -= 1L;
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
  public long getId(OSHDBBoundingBox bbx) {
    return getId(bbx.getMinLonLong(), bbx.getMinLatLong());
  }

  /**
   * Returns width (and height) of cells in given Grid.
   *
   * @return length in degree of borders of cells
   */
  public double getCellWidth() {
    return cellWidth * OSHDB.GEOM_PRECISION;
  }

  /**
   * Calculates BBOX of given Cell.
   *
   * @param cellId ID of a cell calculated by getID
   * @return a BBOX for that cell
   */
  public OSHDBBoundingBox getCellDimensions(final long cellId) {

    //calculate the row and column, this tile-value corresponds to
    int x = (int) (cellId % zoompow);
    int y = (int) ((cellId - x) / zoompow);
    //calculate the values of the south-western most corner
    long lon = (long) ((x * cellWidth) - (180.0 * OSHDB.GEOM_PRECISION_TO_LONG));
    long lat = (long) ((y * cellWidth) - (90.0 * OSHDB.GEOM_PRECISION_TO_LONG));

    final long minlong = lon;
    final long maxlong = (long) (lon + cellWidth) - 1L;

    final long minlat;
    final long maxlat;
    if (zoom == 0) {
      minlat = (long) (-90.0 * OSHDB.GEOM_PRECISION_TO_LONG);
      maxlat = (long) (90.0 * OSHDB.GEOM_PRECISION_TO_LONG);
    } else if (lat == ((long) (90.0 * OSHDB.GEOM_PRECISION_TO_LONG) - cellWidth)) {
      minlat = lat;
      maxlat = (long) (90.0 * OSHDB.GEOM_PRECISION_TO_LONG);
    } else {
      minlat = lat;
      maxlat = (long) (lat + cellWidth) - 1L;
    }
    return new OSHDBBoundingBox(minlong, minlat, maxlong, maxlat);
  }

  /**
   * Returns number of Cells within given BBOX.
   *
   * @param data BBOX to estimate number of cells for
   * @return the long
   */
  public long getEstimatedIdCount(final OSHDBBoundingBox data) {
    //number of Cells in x * number of cells in y
    return Math.max(
        (long) Math.ceil(data.getMaxLonLong() / cellWidth) - (long) Math.floor(data.getMinLonLong() / cellWidth),
        (long) Math.ceil(data.getMaxLatLong() / cellWidth) - (long) Math.floor(data.getMinLatLong() / cellWidth)
    );
  }

  /**
   * getter for zoomlevel.
   *
   * @return zoomlevel
   */
  public int getLevel() {
    return zoom;
  }
  
  public static class IdRange implements Comparable<IdRange>, Serializable {

    private static final long serialVersionUID = 371851731642753753L;

    public static final IdRange INVALID = new IdRange(-1L,-1L);

    private final long start;
    private final long end;

    public static IdRange of(long start, long end) {
      return new IdRange(start,end);
    }

    private IdRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }

    @Override
    public int hashCode() {
      return Objects.hash(end, start);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof IdRange)) {
        return false;
      }
      IdRange other = (IdRange) obj;
      return end == other.end && start == other.start;
    }

    @Override
    public int compareTo(IdRange o) {
      int c = Long.compare(start, o.start);
      if (c == 0) {
        c = Long.compare(end, o.end);
      }
      return c;
    }
  }

  /**
   * Calculates all tiles, that lie within a bounding-box.
   *
   * TODO but priority 999: Add possibility to snap the BBX to the tile-grid.
   * TODO: is an exception needed?
   *
   * @param bbox The bounding box. First dimension is longitude, second is latitude.
   * @param enlarge if true, the BBOX is enlarged by one tile to the south-west (bottom-left)
   *        direction, if false only holds tiles that intersect with the given BBOX.
   * @return Returns a set of Tile-IDs that lie within the given BBOX.
   */
  public Set<IdRange> bbox2CellIdRanges(OSHDBBoundingBox bbox, boolean enlarge) {
    //initialise basic variables
    Set<IdRange> result = new TreeSet<>();
    long minlong = bbox.getMinLonLong();
    long minlat = bbox.getMinLatLong();
    long maxlong = bbox.getMaxLonLong();
    long maxlat = bbox.getMaxLatLong();

    if (minlat > maxlat) {
      LOG.warn("The minimum values are not smaller than the maximum values. This might throw an exeption one day?");
      return null;
    }

    IdRange outofboundsCell = IdRange.INVALID;
    //test if bbox is on earth or extends further
    if (minlong < coordinateToLong(-180.0) || minlong > coordinateToLong(180.0)) {
      result.add(outofboundsCell);
      minlong = coordinateToLong(-180.0);
    }
    if (minlat < coordinateToLong(-90.0) || minlat > coordinateToLong(90.0)) {
      result.add(outofboundsCell);
      minlat = coordinateToLong(-90.0);
    }
    if (maxlong > coordinateToLong(180.0) || maxlong < coordinateToLong(-180.0)) {
      result.add(outofboundsCell);
      maxlong = coordinateToLong(180.0);
    }
    if (maxlat > coordinateToLong(90.0) || maxlat < coordinateToLong(-90.0)) {
      result.add(outofboundsCell);
      maxlat = coordinateToLong(90.0);
    }

    if (minlong == coordinateToLong(180.0)) {
      minlong = coordinateToLong(180.0) - 1L;
    }
    if (maxlong == coordinateToLong(180.0)) {
      maxlong = coordinateToLong(180.0) - 1L;
    }
    if (minlat == coordinateToLong(90.0)) {
      minlat = coordinateToLong(90.0) - 1L;
    }
    if (maxlat == coordinateToLong(90.0)) {
      maxlat = coordinateToLong(90.0) - 1L;
    }

    //cope with BBOX extending over the date-line
    if (minlong > maxlong) {
      result.addAll(bbox2CellIdRanges(
          new OSHDBBoundingBox(
              minlong, minlat,
              coordinateToLong(180.0) - 1L, maxlat
          ), enlarge));

      minlong = coordinateToLong(-180.0);
    }

    //At this point the following should be true
    //minlong[-180.0:179.999999]<=maxlong[-180.0:179.9999]
    //minlat[0:89.99999999999]<=maxlat[0:89.99999999999]
    //calculate column and row range
    int columnmin = (int) ((minlong + coordinateToLong(180.0)) / cellWidth);
    int columnmax = (int) ((maxlong + coordinateToLong(180.0)) / cellWidth);
    int rowmin = (int) ((minlat + coordinateToLong(90.0)) / cellWidth);
    int rowmax = (int) ((maxlat + coordinateToLong(90.0)) / cellWidth);

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
      result.add(IdRange.of(row * zoompow + columnmin, row * zoompow + columnmax));
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
  public Set<IdRange> getNeighbours(CellId center) {
    if (center.getZoomLevel() != this.zoom) {
      //might return neighbours in current zoomlevel given the bbox of the provided CellId one day
      return null;
    }

    OSHDBBoundingBox bbox = this.getCellDimensions(center.getId());
    long minlong = bbox.getMinLonLong() - 1L;
    long minlat = bbox.getMinLatLong() - 1L;
    long maxlong = bbox.getMaxLonLong() + 1L;
    long maxlat = bbox.getMaxLatLong() + 1L;
    OSHDBBoundingBox newbbox = new OSHDBBoundingBox(minlong, minlat, maxlong, maxlat);

    return this.bbox2CellIdRanges(newbbox, false);
  }

}
