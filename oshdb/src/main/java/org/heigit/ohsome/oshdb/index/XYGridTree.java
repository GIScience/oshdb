package org.heigit.ohsome.oshdb.index;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.index.XYGrid.IdRange;
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
  @SuppressWarnings("Convert2Lambda")
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
            return new CellId(gridMap.get(level).getLevel(),
                gridMap.get(level).getId(longitude, latitude));
          }
        };

        return result;
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
    return this.getIds((long) longitude * OSHDB.GEOM_PRECISION_TO_LONG,
        (long) latitude * OSHDB.GEOM_PRECISION_TO_LONG);

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
        return new CellId(i, gridMap.get(i).getId(bbox.getMinLonLong(), bbox.getMinLatLong()));
      }
    }
    return null;
  }

  /**
   * Query cells for given bbox. The boundingbox is automatically enlarged, so lines and relations
   * are included.
   *
   * @param bbox {@code OSHDBBoundingBox} for the query
   * @return
   */
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox bbox) {
    return bbox2CellIds(bbox, false);
  }

  /**
   * Get CellIds in all zoomlevel for a given bbox.
   *
   * @param bbox {@code OSHDBBoundingBox} for the query
   * @param enlarge {@code true} if the query should include enlarged bboxes
   * @return
   */
  @SuppressWarnings("Convert2Lambda")
  public Iterable<CellId> bbox2CellIds(final OSHDBBoundingBox bbox, final boolean enlarge) {
    return new Iterable<CellId>() {
      @Override
      public Iterator<CellId> iterator() {
        return new Iterator<CellId>() {
          private int level = 0;
          private Iterator<IdRange> rows =
              gridMap.get(level).bbox2CellIdRanges(bbox, enlarge).iterator();
          private IdRange row = rows.next();
          private long maxId = row.getEnd();
          private long currId = row.getStart() - 1;

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
        };
      }
    };
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
   * @return
   */
  @SuppressWarnings("Convert2Lambda")
  public Iterable<CellIdRange> bbox2CellIdRanges(final OSHDBBoundingBox bbox,
      final boolean enlarge) {
    return new Iterable<CellIdRange>() {
      @Override
      public Iterator<CellIdRange> iterator() {
        return new Iterator<CellIdRange>() {
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
    };
  }

}
