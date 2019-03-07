package org.heigit.bigspatialdata.oshdb.index;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

/**
 * Multi zoomlevel functionality for the XYGrid.
 */
public class XYGridTree implements Serializable {
  private static final long serialVersionUID = 1L;
  private final int maxLevel;
  private final Map<Integer, XYGrid> gridMap = new TreeMap<>();

  /**
   * Initialises all zoomlevel up until the given one.
   *
   * @param maxzoom the maximum zoom to be used
   */
  public XYGridTree(int maxzoom) {
    maxLevel = maxzoom;
    for (int i=0; i<=maxzoom; i++) {
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
          private int level = -1;

          @Override
          public boolean hasNext() {
            return level < maxLevel;
          }

          @Override
          public CellId next() {
            level++;
            return new CellId(gridMap.get(level).getLevel(), gridMap.get(level).getId(longitude, latitude));
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
    for (int i=maxLevel; i>=0; i--) {
      if (gridMap.get(i).getEstimatedIdCount(bbox) > 2) {
        continue;
      }
      return new CellId(i, gridMap.get(i).getId(bbox.getMinLonLong(), bbox.getMinLatLong()));
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
   * @param BBOX
   * @param enlarge
   * @return
   */
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox BBOX, final boolean enlarge) {
    return new Iterable<CellId>() {
      @Override
      public Iterator<CellId> iterator() {
        return new Iterator<CellId>() {
          private int level = 0;
          private Iterator<Pair<Long, Long>> rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();
          private Pair<Long, Long> row = rows.next();
          private Long maxID = row.getRight();
          private Long currID = row.getLeft() - 1;

          @Override
          public boolean hasNext() {
            if (level < maxLevel) {
              return true;
            }
            if (rows.hasNext()) {
              return true;
            }
            return currID < maxID;
          }

          @Override
          public CellId next() {
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
            level++;
            rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();
            row = rows.next();
            currID = row.getLeft();
            maxID = row.getRight();
            return new CellId(level, currID);
          }
        };
      }
    };
  }

  /**
   * Get CellIds in all zoomlevel for a given BBOX.
   *
   * @param BBOX
   * @param enlarge
   * @return
   */
  public Iterable<Pair<CellId, CellId>> bbox2CellIdRanges(final OSHDBBoundingBox BBOX, final boolean enlarge) {
    return (Iterable<Pair<CellId, CellId>> & Serializable) () -> new Iterator<Pair<CellId, CellId>>() {
      private int level = 0;
      private Iterator<Pair<Long, Long>> rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();

      @Override
      public boolean hasNext() {
        return level < maxLevel || rows.hasNext();
      }

      @Override
      public Pair<CellId, CellId> next() {
        if (!rows.hasNext()) {
          level++;
          rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();
        }
        Pair<Long, Long> row = rows.next();
        return new ImmutablePair<>(
            new CellId(level, row.getLeft()),
            new CellId(level, row.getRight())
        );
      }
    };
  }

}
