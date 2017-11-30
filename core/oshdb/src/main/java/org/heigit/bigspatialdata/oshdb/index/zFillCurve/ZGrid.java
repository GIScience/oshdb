package org.heigit.bigspatialdata.oshdb.index.zFillCurve;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZGrid {

  private static final Logger LOG = LoggerFactory.getLogger(ZGrid.class);

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
   * Returns the cell-Id for given x and y values
   * <br><br>
   *
   * @param x
   * @param y
   * @return cell Id
   */
  public static long getIdXY(int x, int y) {
    // check the size of x and y (in bits), to make only necessary iterations in the following for-loop 
    int max = Math.max(x, y);
    int n = 0;
    while (max > 0) {
      n++;
      max >>= 1;
    }
    // calculate Morton cellId
    int mask = 1;
    long z = 0;
    for (int i = 0; i < n; i++) {
      z |= (x & mask) << i;

      z |= (y & mask) << 1 + i;

      mask <<= 1;

    }
    return z;
  }

  /**
   * Returns the parent cell-Id for a given ID
   * <br><br>
   *
   * @param cell Id
   * @return parent-cell-Id
   */
  public static long getParent(long cell) {
    long parent = (cell >> 2) & 0x3FFFFFFF;
    return parent;
  }

  /**
   * Returns the Moser de-Brujin x and y value for a given ID
   * <br><br>
   * with getX() or getY() you can check the values
   *
   * @param cellID
   * @return XY(MoserX, MoserY)
   */
  public static XY getmXmY(long cellID) {
    // how to get the mX value: https://en.wikipedia.org/wiki/Moser%E2%80%93de_Bruijn_sequence
    int mX = (int) (cellID & 0x55555555);
    int mY = (int) (cellID - mX);
    return new XY(mX, mY);
  }

  /**
   * Returns the x and y coordinate for a given ID
   * <br><br>
   * found here:
   * https://gist.github.com/JLChnToZ/ec41b1b45987d0e1b40ceabc13920559
   *
   * @param cellID
   * @return XY(x,y)
   */
  public static XY getXY(int cellID) {
    int MaxValue = Integer.MAX_VALUE;
    int x = 0;
    int y = 0;

    if (cellID <= 0) {
      return null;
    }
    for (int mask = 1, offset = 0; cellID >= 1 << offset + 1 && mask < MaxValue; mask <<= 1) {
      x |= cellID >> offset++ & mask;
      y |= cellID >> offset & mask;
    }

    return new XY(x, y);
  }

  /**
   * Returns the surrounding neighbor cell-Ids for a given ID
   * <br><br>
   *
   * @param cellID
   * @return [sorted list of neighbor-cell-Ids]
   */
  public static ArrayList<Integer> getNeighbours(int cellID) {
    // result list
    ArrayList<Integer> neighbors = new ArrayList<>();
    // grab coordinates of given Id
    XY coordinates = getXY(cellID);

    // get x coordinate
    int x = coordinates.getX();
    // get y coordinate
    int y = coordinates.getY();

    // go through neighbour x values of given one, from one left to one right of the given x
    for (int i = x - 1; i <= (x + 1); i++) {
      // in each of the 3 columns of x, go through the y values from one above to one below of the given y
      int j = y - 1;
      while (j <= y + 1) {
        // get the Ids of the x y pairs
        long getID = getIdXY(i, j);
        // the given Id is not a neighbour
        if (getID != cellID) {
          // collect the neighbour Ids
          neighbors.add((int) getID);
        }
        j += 1;
      }
    }
    // sorted list of the neighbors
    Collections.sort(neighbors);
    return neighbors;
  }

  public static void main(String[] args) {

    int x = 1;
    int y = 5;

    ZGrid grid = new ZGrid(2);
    long cell = getIdXY(x, y);
    System.out.println("getidxy ");
    System.out.println(cell);
    long parent = getParent(cell);
    System.out.println("parent getidxy");
    System.out.println(parent);
    System.out.printf("mx %s,my %s\n", getmXmY(24).getX(), getmXmY(24).getY());

    System.out.printf("x %s,y %s\n", getXY(24).getX(), getXY(24).getY());

    System.out.println("neighbours");
    System.out.println(getNeighbours(24));

    System.out.println("randfall");
    System.out.println(grid.getIdCoords(180.0, 90.0));

    long testid = grid.getIdCoords(1.0, 89.0);
    System.out.println("getParent getidcoords");
    long parenttestid = getParent(testid);
    System.out.println(parenttestid);

    System.out.println("in der bbox:");
    BoundingBox box = new BoundingBox(-91.0, 170.0, -89.0, 1.0);
    System.out.println(grid.bbox2CellId(box, false));

    System.out.println("test");

    try {
      grid.test();
    } catch (ZGridException e) {
    }

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
  public ZGrid(final int zoom) {
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
   * Returns the cell-Id for given coordinates
   * <br><br>
   *
   * @param longitude
   * @param latitude
   * @return cell Id
   */
  public long getIdCoords(double longitude, double latitude) {
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
    final int y = (int) (latitude / (cellWidth));
    // calculate Morton cellId
    int mask = 1;
    long z = 0;
    for (int i = 0; i < 32; i++) {
      z |= (x & mask) << i;
      z |= (y & mask) << 1 + i;
      mask <<= 1;
    }
    return z;
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
  public Set< Long> bbox2CellId(BoundingBox bbox, boolean enlarge) {
    //initialise basic variables
    Set< Long> result = new TreeSet<>();

    double minlong = bbox.getMinLon();
    double minlat = bbox.getMinLat();
    double maxlong = bbox.getMaxLon();
    double maxlat = bbox.getMaxLat();

    if (minlat > maxlat) {
      LOG.warn("The minimum values are not smaller than the maximum values. This might throw an exeption one day?");
      return null;
    }

    long outofboundsCell = -1L;
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
      minlong = -180.0;
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
      result.addAll(bbox2CellId(new BoundingBox(minlong, 180.0 - EPSILON, minlat, maxlat), enlarge));

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
      //it is impossible for features to span over the datelimit, so the enlargement stops at column 0
      if (columnmin > 0) {
        columnmin -= 1;
      }
      if (rowmin > 0) {
        rowmin -= 1;
      }
    }

    // add cellIDs to result Set 
    for (int row = rowmin; row <= rowmax; row++) {
      // CellID of min X
      result.add(getIdXY(columnmin, row));
      // calculate Ids between columnmin and columnmax, if the difference between them is bigger than one cell
      int columnmin2 = columnmin;
      while ((columnmin2 + 1) < columnmax) {
        columnmin2 += 1;
        result.add(getIdXY(columnmin2, row));
      }
      // CellID of max X
      result.add(getIdXY(columnmax, row));
    }
    return result;
  }

  /**
   * Helper method <br><br>
   * checks if the given bbox size is passed and if so, splits up the given bbox
   * into two new ones<br>
   * if the bbox has the coordinates -91.0,190.0,-89.0,89.0, it will return one
   * box with -91,00,180,00, -89,00,89,00 and one with -180,00,-170,00,
   * -89,00,89,00
   *
   * <br><br>
   *
   * @param
   * @return Returns a list of Bounding Boxes
   * @throws ZGridException
   */
  private LinkedList<BoundingBox> bboxSplitter(BoundingBox bbox, boolean enlarge) throws ZGridException {
    //System.out.printf("bbox %1$,.2f,%2$,.2f, %3$,.2f,%4$,.2f\n",bbox.minLon,bbox.maxLon,bbox.minLat,bbox.maxLat);
    if (bbox.getMinLon() > bbox.getMaxLon()) {
      throw new ZGridException("!Bounding Box not valid: minLon is larger than maxLon!");
    } else if (bbox.getMinLat() > bbox.getMaxLat()) {
      throw new ZGridException("!Bounding Box not valid: minLat is larger than maxLat!");
    }

    LinkedList<BoundingBox> collectBboxes = new LinkedList<>();
    if (bbox.getMinLon() < -180.0) {

      double newMinLon = 180 + (bbox.getMinLon() + 180);
      BoundingBox newBbox = new BoundingBox(newMinLon, 180.0 - EPSILON, bbox.getMinLat(), bbox.getMaxLat());
      BoundingBox otherBbox = new BoundingBox(-180.0, bbox.getMaxLon(), bbox.getMinLat(), bbox.getMaxLat());
      collectBboxes.add(newBbox);
      collectBboxes.add(otherBbox);

    } else if (bbox.getMaxLon() > 180.0) {

      double newMaxLon = -180 + (bbox.getMaxLon() - 180);
      BoundingBox newBbox = new BoundingBox(-180.0, newMaxLon, bbox.getMinLat(), bbox.getMaxLat());
      BoundingBox otherBbox = new BoundingBox(bbox.getMinLon(), 180.0 - EPSILON, bbox.getMinLat(), bbox.getMaxLat());
      collectBboxes.add(newBbox);

      //System.out.printf("newBbox %1$,.2f,%2$,.2f, %3$,.2f,%4$,.2f\n",newBbox.minLon,newBbox.maxLon,newBbox.minLat,newBbox.maxLat);
      collectBboxes.add(otherBbox);
      //System.out.printf("otherBbox %1$,.2f,%2$,.2f, %3$,.2f,%4$,.2f\n",otherBbox.minLon,otherBbox.maxLon,otherBbox.minLat,otherBbox.maxLat);

    }
    //System.out.println("boxSplitter");

    return collectBboxes;
  }

  /**
   * Helper method <br><br>
   * iterator for cellIDs in a given Bbox
   *
   * <br>
   *
   * @param
   * @return Long[Zoom + CellIds] (Zoom is stored in the last byte of the Long)
   */
  private Iterator<Long> simpleIterator(BoundingBox bbox) {

    Iterator<Long> result = new Iterator<Long>() {

      double minlong = bbox.getMinLon();
      double minlat = bbox.getMinLat();
      double maxlong = bbox.getMaxLon();
      double maxlat = bbox.getMaxLat();

      int columnmin = (int) ((minlong + 180.0) / cellWidth);
      int columnmax = (int) ((maxlong + 180.0) / cellWidth);
      int rowmin = (int) ((minlat + 90.0) / cellWidth);
      int rowmax = (int) ((maxlat + 90.0) / cellWidth);

      int x = columnmax;
      int y = rowmin - 1;

      @Override
      public boolean hasNext() {
        boolean r;
        r = x < columnmax || (x == columnmax && y < rowmax);
        return r;
      }

      @Override
      public Long next() {
        int zoomlevel = zoom;
        if (x < columnmax) {
          x += 1;
        } else if (y < rowmax) {
          x = columnmin;
          y += 1;
        }
        System.out.println("in next");
        System.out.printf("x, y in next()+++++++++++++++++++++++++: x:%d,y:%d%n", x, y);
        System.out.println("zoom");
        System.out.println(zoom);
        long xyID = getIdXY(x, y);
        zoomlevel <<= 56;
        xyID |= zoomlevel;
        System.out.println("xy ID");
        System.out.println(Long.toBinaryString(xyID));
        System.out.println("last byte = zoom");
        int n = 3;
        long xcurr = (xyID >> (8 * n)) & 0xff;

        System.out.println(Long.toBinaryString(xcurr));

        System.out.println("first byte = id");
        int n2 = 0;
        long z = (xyID >> (8 * n2)) & 0xff;

        System.out.println(Long.toBinaryString(z));
        return xyID;
      }
    };

    return result;
  }

  /**
   * iterator for cellIDs in a given Bbox
   *
   * <br><br>
   *
   * @param bbox
   * @param enlarge
   * @return Long[Zoom + CellIds] (Zoom is stored in the last byte of the Long)
   * @throws ZGridException
   */
  public Iterator<Long> bbox2CellIdIterator(BoundingBox bbox, boolean enlarge) throws ZGridException {

    BoundingBox bbox2 = bbox;
    // check if given Bbox lays completely outside the normal world coordinates (-180 ... 180)
    // and if so beam the Bbox back into the normal world coordinate system
    if (bbox2.getMinLon() > 180.0) {
      double newMinLon = bbox2.getMinLon();
      newMinLon += 90;
      int m = (int) (bbox2.getMinLon() / 180);
      newMinLon = (bbox2.getMinLon() - m * 180) - 90;
      double newMaxLon = bbox2.getMaxLon() - m * 180;
      bbox2 = new BoundingBox(newMinLon, newMaxLon, bbox2.getMinLat(), bbox2.getMaxLat());
    } else if (bbox2.getMaxLon() < -180.0) {
      double newMaxLon = bbox2.getMinLon();
      newMaxLon += 90;
      int m = (int) (bbox2.getMaxLon() / 180);
      newMaxLon = (bbox2.getMaxLon() - m * 180) - 90;
      double newMinLon = bbox2.getMinLon() - m * 180;
      bbox2 = new BoundingBox(newMinLon, newMaxLon, bbox2.getMinLat(), bbox2.getMaxLat());
    }

    // check if given Bbox is splitted into two Bboxes
    LinkedList<BoundingBox> bboxCount = new LinkedList<>(bboxSplitter(bbox2, enlarge));

    Iterator<BoundingBox> iter = bboxCount.iterator();
    // Iterator for a Bbox which is splitted into two Bboxes
    if (bboxCount.size() > 1) {

      while (iter.hasNext()) {
        //System.out.printf("columnmax, rowmin-1: x:%d,y:%d%n",x,y);
        BoundingBox bbox3 = iter.next();

        return simpleIterator(bbox3);
      }
    }
    return simpleIterator(bbox2);

  }

  /**
   * Collects Long[Zoom + CellIds] which lay in a given Bbox
   * <br> Zoom is stored in the last byte of the Long
   *
   * @param bbox
   * @param enlarge
   * @return Set of Long[Zoom + CellIds]
   */
  public Set<Long> bbox2CellIdSet(BoundingBox bbox, boolean enlarge) {
    Set<Long> result = new TreeSet<>();
    bbox2CellIdIterable(bbox, enlarge).forEach(id -> {
      result.add(id);
    });
    System.out.println("---------------------------------------result of bbox2CellIdSet");
    System.out.println(result);
    return result;
  }

  /**
   * Iterable for Bbox
   *
   * @param bbox
   * @param enlarge
   * @return Long[Zoom + CellIds] (Zoom is stored in the last byte of the Long)
   */
  public Iterable<Long> bbox2CellIdIterable(BoundingBox bbox, boolean enlarge) {
    // ...
    return new Iterable<Long>() {
      @Override
      public Iterator<Long> iterator() {
        try {
          return bbox2CellIdIterator(bbox, enlarge);
        } catch (ZGridException e) {
        }
        return null;
      }
    };
  }

  public Stream<Long> bbox2CellIdStream(BoundingBox bbox, boolean enlarge) {
    return StreamSupport.stream(bbox2CellIdIterable(bbox, enlarge).spliterator(), false);
  }

  public void bbox2CellId(BoundingBox bbox, LongConsumer consumer) {
    // ...
    consumer.accept(123l);
  }

  public void test() throws ZGridException {
    LongConsumer vAnonym = new LongConsumer() {
      @Override
      public void accept(long cellId) {
        // TODO Auto-generated method stub					
      }
    };

    //BoundingBox bbox = new BoundingBox(-91.0,190.0,-89.0,89.0);
    BoundingBox bbox = new BoundingBox(-91.0, 179.0, -89.0, 89.0);
    System.out.printf("+++++++++++++++++++++++++++++++++++++++++++  bbox2CellId\n");

    bbox2CellId(bbox, i -> {
      System.out.println("i");
      System.out.println(i);
    });
    boolean bool = false;
    System.out.printf("+++++++++++++++++++++++++++++++++++++++++++  bbox2CellIdSet\n");
    bbox2CellIdSet(bbox, bool);

    bbox2CellId(bbox, vAnonym);

    //System.out.printf("+++++++++++++++++++++++++++++++++++++++++++ findMaxx\n");
    //findMaxX(bbox, bool);
    System.out.printf("+++++++++++++++++++++++++++++++++++++++++++ bbox2CellIdIterator\n");
    for (Iterator<Long> itr = bbox2CellIdIterator(bbox, bool); itr.hasNext();) {
      Long id = itr.next();
      System.out.println("id");
      System.out.println(id);
    }
    System.out.printf("+++++++++++++++++++++++++++++++++++++++++++ bbox2CellIdIterable\n");
    bbox2CellIdIterable(bbox, bool).forEach(id -> {
      System.out.println("id iterable");
      System.out.println(id);
    });
    System.out.println("CellIdStream");
    System.out.printf("+++++++++++++++++++++++++++++++++++++++++++ bbox2CellIdStream\n");
    bbox2CellIdStream(bbox, bool)
            .filter(id -> id % 2 == 0)
            .forEach(System.out::println);
  }

  /**
   * helper class holding two values, x and y
   * <br>
   * <br><br>
   * XY(x,y)
   */
  private static class XY {

    public final int x;
    public final int y;

    XY(int x, int y) {
      this.x = x;
      this.y = y;
    }

    /**
     * get the x value of a XY
     * <br>
     *
     * @return x
     */
    public int getX() {
      return this.x;
    }

    /**
     * get the y value of a XY
     * <br>
     *
     * @return y
     */
    public int getY() {
      return this.y;
    }

  }

  /**
   *
   */
  @SuppressWarnings("serial")
  class ZGridException extends Exception {

    ZGridException(String msg) {
      super(msg);
    }
  }

}
