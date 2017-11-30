package org.heigit.bigspatialdata.oshdb.index;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi zoomlevel functionality for the XYGrid.
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class XYGridTree {

  private static final Logger LOG = LoggerFactory.getLogger(XYGridTree.class);

  private static final double EPSILON = 1e-11;
  private final int maxLevel;
  private final Map<Integer, XYGrid> gridMap = new TreeMap<>();

  /**
   * Initialises all zoomlevel above the given one.
   *
   * @param maxzoom the maximum zoom to be used
   */
  public XYGridTree(int maxzoom) {
    maxLevel = maxzoom;
    for (int i = maxzoom; i > 0; i--) {
      gridMap.put(i, new XYGrid(i));
    }

  }

  public XYGridTree() {
    this(OSHDB.MAXZOOM);
  }

  /**
   * Get CellIds in all zoomlevel for a given point.
   *
   * @param longitude
   * @param latitude
   * @return An iterator over the cellIds in all zoomlevel
   */
  public Iterable<CellId> getIds(double longitude, double latitude) {

    return new Iterable<CellId>() {

      @Override
      public Iterator<CellId> iterator() {
        Iterator<CellId> result = new Iterator<CellId>() {
          private int level = maxLevel + 1;

          @Override
          public boolean hasNext() {
            return level > 1;
          }

          @Override
          public CellId next() {
            try {
              level--;
              return new CellId(gridMap.get(level).getLevel(), gridMap.get(level).getId(longitude, latitude));
            } catch (CellId.cellIdExeption ex) {
              LOG.error(ex.getMessage());
              return null;
            }
          }
        };

        return result;
      }
    };

  }

  /**
   * Calculate cell, a line or relation should be stored in.
   *
   * @param bbox
   * @return
   */
  public CellId getInsertId(BoundingBox bbox) {
    for (int i = maxLevel - 1; i > 0; i--) {
      try {
        if (gridMap.get(i).getEstimatedIdCount(bbox) > 4) {
          continue;
        }
        return new CellId(i, gridMap.get(i).getId(bbox.minLon, bbox.minLat));
      } catch (CellId.cellIdExeption ex) {
        LOG.error(ex.getMessage());
        return null;
      }
    }
    return null;
  }

  /**
   * Query cells for given BBOX. The boundingbox is automatically enlarged, so
   * lines and relations are included.
   *
   * @param BBOX
   * @return
   */
  public Iterable<CellId> bbox2CellIds(final BoundingBox BBOX) {
    return bbox2CellIds(BBOX, true);
  }

  /**
   * Get CellIds in all zoomlevel for a given BBOX.
   *
   *
   * @param BBOX
   * @param enlarge
   * @return
   */
  public Iterable<CellId> bbox2CellIds(final BoundingBox BBOX, final boolean enlarge) {

    return new Iterable<CellId>() {
      @Override
      public Iterator<CellId> iterator() {
        Iterator<CellId> result = new Iterator<CellId>() {
          private int level = maxLevel;
          private Iterator<Pair<Long, Long>> rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();
          private Pair<Long, Long> row = rows.next();
          private Long maxID = row.getRight();
          private Long currID = row.getLeft() - 1;

          @Override
          public boolean hasNext() {
            if (level > 1) {
              return true;
            }
            if (rows.hasNext()) {
              return true;
            }
            return currID < maxID;
          }

          @Override
          public CellId next() {
            try {
              if (currID < maxID) {
                currID++;
                return new CellId(level, currID);
              }
              if (rows.hasNext()) {
                row = rows.next();
                currID = row.getLeft();
                maxID = row.getRight();
                return new CellId(level, currID);
              }
              level--;
              rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();
              row = rows.next();
              currID = row.getLeft();
              maxID = row.getRight();
              return new CellId(level, currID);
            } catch (CellId.cellIdExeption ex) {
              LOG.error(ex.getMessage());
              return null;
            }
          }

        };

        return result;
      }
    };

  }

  /**
   * Get 2D neighbours of given cell in its zoomlevel and all other zoomlevel.
   *
   * @param center
   * @return
   */
  public Iterable<CellId> getMultiZoomNeighbours(CellId center) {
    BoundingBox bbox = this.gridMap.get(center.getZoomLevel()).getCellDimensions(center.getId());
    double minlong = bbox.minLon - EPSILON;
    double minlat = bbox.minLat - EPSILON;
    double maxlong = bbox.maxLon + EPSILON;
    double maxlat = bbox.maxLat + EPSILON;
    BoundingBox newbbox = new BoundingBox(minlong, maxlong, minlat, maxlat);
    return this.bbox2CellIds(newbbox, false);
  }

  /**
   * Get the parent CellIds in all other zoomlevel.
   *
   * @param center
   * @return
   */
  public Iterable<CellId> getMultiZoomParents(CellId center) {
    return this.bbox2CellIds(this.gridMap.get(center.getZoomLevel()).getCellDimensions(center.getId()), false);
  }

}
