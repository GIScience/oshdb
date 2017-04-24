package org.heigit.bigspatialdata.hosmdb.util;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Multi zoomlevel functionality for the XYGrid.
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class XYGridTree {

  private static final Logger LOG = Logger.getLogger(XYGridTree.class.getName());
  private final Map<Integer, XYGrid> gridMap = new TreeMap<>();

  /**
   * Initialises all zoomlevel above the given one.
   *
   * @param maxzoom the maximum zoom to be used
   */
  public XYGridTree(int maxzoom) {
    for (int i = maxzoom; i >= 0; i--) {
      gridMap.put(i, new XYGrid(i));
    }

  }

  /**
   * Get CellIds in all zoomlevel for a given point.
   *
   * @param longitude
   * @param latitude
   * @return An iterator over the cellIds in all zoomlevel
   */
  public Iterator<CellId> getIds(double longitude, double latitude) {

    @SuppressWarnings("unchecked")
    Iterator<CellId> result = new Iterator() {
      private final int maxLevel = gridMap.size();
      private int level = maxLevel;

      @Override
      public boolean hasNext() {
        return level > 0;
      }

      @Override
      public Object next() {
        try {
          level--;
          return new CellId(gridMap.get(level).getLevel(), gridMap.get(level).getId(longitude, latitude));

        } catch (CellId.cellIdExeption ex) {
          LOG.log(Level.SEVERE, ex.getMessage());
          return null;
        }
      }
    };

    return result;
  }

  /**
   * Calculate cell, a line or relation should be stored in.
   *
   * @param bbox
   * @return
   */
  protected CellId getInsertId(BoundingBox bbox) {
    int maxLevel = gridMap.size();
    MultiDimensionalNumericData mdBbox = new BasicNumericDataset(new NumericData[]{new NumericRange(bbox.minLon, bbox.maxLon), new NumericRange(bbox.minLat, bbox.maxLat)});
    for (int i = maxLevel - 1; i >= 0; i--) {
      try {
        if (gridMap.get(i).getEstimatedIdCount(mdBbox) > 4) {
          continue;
        }
        return new CellId(i, gridMap.get(i).getId(bbox.minLon, bbox.minLat));
      } catch (CellId.cellIdExeption ex) {
        LOG.log(Level.SEVERE, ex.getMessage());
        return null;
      }
    }
    return null;
  }

  /**
   * Get CellIds in all zoomlevel for a given BBOX.
   *
   *
   * @param BBOX
   * @param enlarge
   * @return
   */
  public Iterator<CellId> bbox2CellIds(final MultiDimensionalNumericData BBOX, final boolean enlarge) {

    @SuppressWarnings("unchecked")
    Iterator<CellId> result = new Iterator() {

      private final int maxLevel = gridMap.size() - 1;
      private int level = maxLevel;
      private Iterator<Pair<Long, Long>> rows = gridMap.get(level).bbox2CellIdRanges(BBOX, enlarge).iterator();
      private Pair<Long, Long> row = rows.next();
      private Long maxID = row.getRight();
      private Long currID = row.getLeft() - 1;

      @Override
      public boolean hasNext() {
        if (level > 0) {
          return true;
        }
        if (rows.hasNext()) {
          return true;
        }
        return currID < maxID;
      }

      @Override
      public Object next() {
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
          LOG.log(Level.SEVERE, ex.getMessage());
          return null;
        }
      }

    };

    return result;

  }

  /**
   * Get CellIds in all zoomlevel for a given BBOX.
   *
   * @param bbox
   * @param enlarge
   * @return
   */
  public Iterator<CellId> bbox2CellIds(BoundingBox bbox, boolean enlarge) {
    MultiDimensionalNumericData mdBbox = new BasicNumericDataset(new NumericData[]{new NumericRange(bbox.minLon, bbox.maxLon), new NumericRange(bbox.minLat, bbox.maxLat)});
    return this.bbox2CellIds(mdBbox, enlarge);
  }

}
