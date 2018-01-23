package org.heigit.bigspatialdata.oshdb.index;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
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
  public Iterable<CellId> getIds(long longitude, long latitude) {
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
   * Get CellIds in all zoomlevel for a given point.
   *
   * @param longitude
   * @param latitude
   * @return An iterator over the cellIds in all zoomlevel
   */
  public Iterable<CellId> getIds(double longitude, double latitude) {
    return this.getIds((long) longitude * OSHDB.GEOM_PRECISION_TO_LONG, (long) latitude * OSHDB.GEOM_PRECISION_TO_LONG);

  }

  /**
   * Calculate cell, a line or relation should be stored in.
   *
   * @param bbox
   * @return
   */
  public CellId getInsertId(OSHDBBoundingBox bbox) {
    for (int i = maxLevel; i > 0; i--) {
      try {
        if (gridMap.get(i).getEstimatedIdCount(bbox) > 2) {
          continue;
        }
        return new CellId(i, gridMap.get(i).getId(bbox.minLon, bbox.minLat));
      } catch (CellId.cellIdExeption ex) {
        LOG.error("", ex);
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
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox BBOX) {
    return bbox2CellIds(BBOX, false);
  }

  /**
   * Get CellIds in all zoomlevel for a given BBOX.
   *
   *
   * @param BBOX
   * @param enlarge
   * @return
   */
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox BBOX, final boolean enlarge) {

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
    OSHDBBoundingBox bbox = this.gridMap.get(center.getZoomLevel()).getCellDimensions(center.getId());
    long minlong = bbox.minLon - 1L;
    long minlat = bbox.minLat - 1L;
    long maxlong = bbox.maxLon + 1L;
    long maxlat = bbox.maxLat + 1L;
    OSHDBBoundingBox newbbox = new OSHDBBoundingBox(minlong, minlat, maxlong, maxlat);
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
