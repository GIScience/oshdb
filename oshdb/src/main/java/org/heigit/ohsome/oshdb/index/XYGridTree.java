package org.heigit.ohsome.oshdb.index;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.index.XYGrid.IdRange;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;
import org.heigit.ohsome.oshdb.util.CellId;

/**
 * Multi zoomlevel functionality for the XYGrid.
 */
@SuppressWarnings("checkstyle:abbreviationAsWordInName")
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
    for (int i = 0; i <= maxzoom; i++) {
      gridMap.put(i, new XYGrid(i));
    }
  }

  public XYGridTree() {
    this(OSHDB.MAXZOOM);
  }

  /**
   * Get CellIds in all zoomlevel for a given point.
   *
   * @param longitude Longiude for the given point
   * @param latitude Latitude for the given point
   * @return An iterator over the cellIds in all zoomlevel
   */
  public Iterable<CellId> getIds(long longitude, long latitude) {
    return (Iterable<CellId> & Serializable) () -> new Iterator<>() {
          private int level = -1;

          @Override
          public boolean hasNext() {
            return level < maxLevel;
          }

          @Override
          public CellId next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            level++;
            return new CellId(gridMap.get(level).getLevel(),
                gridMap.get(level).getId(longitude, latitude));
          }
    };
  }

  /**
   * Get CellIds in all zoomlevel for a given point.
   *
   * @param longitude Longitude for the given point
   * @param latitude Latitude for the given point
   * @return An iterator over the cellIds in all zoomlevel
   */
  public Iterable<CellId> getIds(double longitude, double latitude) {
    return this.getIds(OSMCoordinates.toOSM(longitude),
        OSMCoordinates.toOSM(latitude));

  }

  /**
   * Calculate cell, a line or relation should be stored in.
   *
   * @param bbox {@code OSHDBoundingBox} for which to get the {@code CellId}
   * @return {@code CellId} for the given {@code OSHDBBoundingbox}
   */
  public CellId getInsertId(OSHDBBoundingBox bbox) {
    for (int i = maxLevel; i >= 0; i--) {
      if (gridMap.get(i).getEstimatedIdCount(bbox) <= 2) {
        return new CellId(i, gridMap.get(i).getId(bbox.getMinLongitude(), bbox.getMinLatitude()));
      }
    }
    return null;
  }

  /**
   * Query cells for given bbox. The boundingbox is automatically enlarged, so lines and relations
   * are included.
   *
   * @param bbox {@code OSHDBBoundingBox} for the query
   */
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox bbox) {
    return bbox2CellIds(bbox, false);
  }

  /**
   * Get CellIds in all zoomlevel for a given bbox.
   *
   * @param bbox {@code OSHDBBoundingBox} for the query
   * @param enlarge {@code true} if the query should include enlarged bboxes
   */
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox bbox, final boolean enlarge) {
    return new CellIdIterable(gridMap, bbox, enlarge, maxLevel);
  }

  private static class CellIdIterable implements Iterable<CellId>, Serializable {
    private final Map<Integer, XYGrid> gridMap;
    private final OSHDBBoundingBox bbox;
    private final boolean enlarge;
    private final int maxLevel;

    private CellIdIterable(Map<Integer, XYGrid> gridMap, OSHDBBoundingBox bbox, boolean enlarge,
        int maxLevel) {
      this.gridMap = gridMap;
      this.bbox = bbox;
      this.enlarge = enlarge;
      this.maxLevel = maxLevel;
    }

    @Override
    public Iterator<CellId> iterator() {
      return new CellIdIterator();
    }

    private class CellIdIterator implements Iterator<CellId>, Serializable {
      private Iterator<IdRange> rows;
      private int level;
      private IdRange row;
      private long maxId;
      private long currId;


      private CellIdIterator() {
        this.level = 0;
        this.rows = gridMap.get(level).bbox2CellIdRanges(bbox, enlarge).iterator();
        this.row = rows.next();
        this.maxId = row.getEnd();
        this.currId = row.getStart() - 1;
      }

      @Override
      public boolean hasNext() {
        if (level < maxLevel) {
          return true;
        }
        if (rows.hasNext()) {
          return true;
        }
        return currId < maxId;
      }

      @Override
      public CellId next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        if (currId < maxId) {
          currId++;
          return new CellId(level, currId);
        }

        if (rows.hasNext()) {
          row = rows.next();
          currId = row.getStart();
          maxId = row.getEnd();
          return new CellId(level, currId);
        }
        level++;
        rows = gridMap.get(level).bbox2CellIdRanges(bbox, enlarge).iterator();
        row = rows.next();
        currId = row.getStart();
        maxId = row.getEnd();
        return new CellId(level, currId);
      }
    }
  }

  public static class CellIdRange implements Serializable {

    private static final long serialVersionUID = -8704075537597232890L;

    private final CellId start;
    private final CellId end;

    public static CellIdRange of(CellId start, CellId end) {
      return new CellIdRange(start, end);
    }

    private CellIdRange(CellId start, CellId end) {
      this.start = start;
      this.end = end;
    }

    public CellId getStart() {
      return start;
    }

    public CellId getEnd() {
      return end;
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, end);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof CellIdRange)) {
        return false;
      }
      CellIdRange other = (CellIdRange) obj;
      return Objects.equals(end, other.end) && Objects.equals(start, other.start);
    }
  }

  /**
   * Get CellIds in all zoomlevel for a given bbox.
   *
   * @param bbox {@code OSHDBBoundingBox}
   * @param enlarge {@code true} to include enlarged bboxes
   * @return List of {@code CellIdRanges} which are covered by the given bbox
   */
  public Iterable<CellIdRange> bbox2CellIdRanges(final OSHDBBoundingBox bbox,
      final boolean enlarge) {
    return (Iterable<CellIdRange> & Serializable) () -> new Iterator<>() {
          private int level = 0;
          private Iterator<IdRange> rows =
              gridMap.get(level).bbox2CellIdRanges(bbox, enlarge).iterator();

          @Override
          public boolean hasNext() {
            return level < maxLevel || rows.hasNext();
          }

          @Override
          public CellIdRange next() {
            if (!rows.hasNext()) {
              level++;
              rows = gridMap.get(level).bbox2CellIdRanges(bbox, enlarge).iterator();
            }
            IdRange row = rows.next();
            return CellIdRange.of(new CellId(level, row.getStart()),
                new CellId(level, row.getEnd()));
          }
    };
  }

}
