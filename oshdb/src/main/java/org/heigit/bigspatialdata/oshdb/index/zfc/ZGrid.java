package org.heigit.bigspatialdata.oshdb.index.zfc;

import java.util.Comparator;
import java.util.Iterator;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox.OVERLAP;

public class ZGrid {

  private static final int DIMENSION = 2;
  private static long ZOOM_FACTOR = 1L << 56;
  private static long ID_MASK = 0x00FFFFFFFFFFFFFFL;

  private static final long space = (long) (360.0 * OSHDB.GEOM_PRECISION_TO_LONG);
  private final int maxZoom;
  private static final OSHDBBoundingBox zeroBoundingBox = new OSHDBBoundingBox(0, 0, 0, 0);

  public ZGrid(int maxZoom) {
    if (maxZoom < 1) {
      throw new IllegalArgumentException("maxZoom must be >= 1 but is " + maxZoom);
    }
    // maxZoom so that 8 Bit ZoomLevel + maxZoom Bits per Dimension(2) = 64 Bit
    // (Long)
    if (maxZoom > 28) {
      throw new IllegalArgumentException("maxZoom must not be > 28 but is " + maxZoom);
    }
    this.maxZoom = maxZoom;
  }

  public long getIdSingleZIdWithZoom(long lon, long lat) {
    return getIdSingleZIdWithZoom(lon, lon, lat, lat);
  }

  public long getIdSingleZIdWithZoom(long[] lon, long[] lat) {
    return getIdSingleZIdWithZoom(lon[0], lon[1], lat[0], lat[1]);
  }

  public long getIdSingleZIdWithZoom(long minLon, long maxLon, long minLat, long maxLat) {
    final long[] x = new long[2];
    final long[] y = new long[2];

    minLon = normalizeLon(minLon);
    maxLon = normalizeLon(maxLon);
    minLat = normalizeLat(minLat);
    maxLat = normalizeLat(maxLat);

    if (!validateLon(minLon) || !validateLon(maxLon)
            || !validateLat(minLat) || !validateLat(maxLat)) {
      return -1;
    }

    int zoom = Math.min(optimalZoom(maxLon - minLon), optimalZoom(maxLat - minLat));
    long zoomPow = (long) Math.pow(2, zoom);

    while (zoom > 0) {
      long cellWidth = space / zoomPow;
      x[0] = minLon / cellWidth;
      x[1] = maxLon / cellWidth;
      y[0] = minLat / cellWidth;
      y[1] = maxLat / cellWidth;

      if (x[0] == x[1] && y[0] == y[1]) {
        break;
      }
      zoom -= 1;
      zoomPow >>>= 1;
    }

    if (zoom == 0) {
      return 0;
    }

    return addZoomToId(morton(x[0], y[0]), zoom);
  }

  public static int getZoom(long zId) {
    return (int) (zId / ZOOM_FACTOR);
  }

  public static long addZoomToId(long id, int zoom) {
    return id + (zoom * ZOOM_FACTOR);
  }

  public static long getIdWithoutZoom(long zId) {
    return zId & ID_MASK;
  }

  public static long getParent(long zId, int parentZoom) {
    final int zoom = getZoom(zId);
    if (zoom < parentZoom) {
      throw new IllegalArgumentException("zoom of id already lesser than parentZoom (zoom:" + zoom + " parentZoom:" + parentZoom + ")");
    }
    if (zoom == parentZoom) {
      return zId;
    }
    final long diff = zoom - parentZoom;
    final long id = (getIdWithoutZoom(zId) >>> diff * 2);

    return addZoomToId(id, parentZoom);
  }

  public static long getParent(long zId) {
    final int zoom = getZoom(zId);
    if (zoom > 0) {
      final long parentId = getIdWithoutZoom(zId) >>> 2;
      return addZoomToId(parentId, zoom - 1);
    }
    return 0;
  }

  public Iterable<Long> iterableDF(OSHDBBoundingBox search) {
    final ZGrid zGrid = this;
    return new Iterable<Long>() {
      final ZGrid grid = zGrid;

      @Override
      public Iterator<Long> iterator() {
        return grid.iteratorDF(search);
      }
    };
  }

  public Iterator<Long> iteratorDF(OSHDBBoundingBox search) {
    return new DFIterator(search, maxZoom, space);
  }

  public static OSHDBBoundingBox getBoundingBox(long zId) {
    if (zId < 0) {
      return zeroBoundingBox;
    }
    final long id = getIdWithoutZoom(zId);
    final int zoom = getZoom(zId);
    final long cellWidth = space / (long) Math.pow(2, zoom);

    long[] xy = getXY(id);

    final long minLon = denormalizeLon(xy[0] * cellWidth);
    final long maxLon = minLon + cellWidth - 1;
    final long minLat = denormalizeLat(xy[1] * cellWidth);
    final long maxLat = minLat + cellWidth - 1;

    return new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat);
  }

  private static long[] getXY(long id) {
    long[] xy = new long[2];
    for (long mask = 1, offset = 0; id >= (1 << offset) && mask < Integer.MAX_VALUE; mask <<= 1) {
      xy[0] |= id >> offset++ & mask;
      xy[1] |= id >> offset & mask;
    }
    return xy;
  }

  private int optimalZoom(long delta) {
    if (delta == 0) {
      return maxZoom;
    }
    return Math.min(63 - Long.numberOfLeadingZeros((space / (delta))), maxZoom);
  }

  private static long morton(long x, long y) {
    // Replaced this line with the improved code provided by Tuska
    // int n = Math.max(Integer.highestOneBit(odd),
    // Integer.highestOneBit(even));
    long max = Math.max(x, y);
    int n = 0;
    while (max > 0) {
      n++;
      max >>= 1;
    }

    long z = 0;
    int mask = 1;
    for (int i = 0; i < n; i++) {
      z |= (x & mask) << i;
      z |= (y & mask) << 1 + i;
      mask <<= 1;
    }
    return z;
  }

  private static long normalizeLon(long lon) {
    return lon + (long) (180.0 * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  private static long denormalizeLon(long lon) {
    return lon - (long) (180.0 * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  private static long normalizeLat(long lat) {
    return lat + (long) (90.0 * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  private static long denormalizeLat(long lat) {
    return lat - (long) (90.0 * OSHDB.GEOM_PRECISION_TO_LONG);
  }

  private static boolean validateLon(long lon) {
    if (lon < 0 || lon > space) {
      return false;
    }
    return true;
  }

  private static boolean validateLat(long lat) {
    if (lat < 0 || lat > space / 2) {
      return false;
    }
    return true;
  }

  public static final Comparator<Long> ORDER_DFS_TOP_DOWN = (a, b) -> {
    if(a == -1)
      return (b == -1)?0:-1;
    if(b == -1)
      return 1;
    
    final long aZ = getZoom(a);
    final long bZ = getZoom(b);
    if (aZ == bZ) {
      return Long.compare(a, b);
    }
    final long deltaZ = Math.abs(aZ - bZ);
    final long aId = getIdWithoutZoom(a);
    final long bId = getIdWithoutZoom(b);
    final long x, y;
    final int prio;
    if (aZ < bZ) {
      x = aId << DIMENSION * deltaZ;
      y = bId;
      prio = -1;
    } else {
      x = aId;
      y = bId << DIMENSION * deltaZ;
      prio = 1;
    }
    final int r = Long.compare(x, y);
    return (r == 0) ? prio : r;
  };

  public static final Comparator<Long> ORDER_DFS_BOTTOM_UP = (a, b) -> {
    final long aZ = getZoom(a);
    final long bZ = getZoom(b);
    if (aZ == bZ) {
      return Long.compare(a, b);
    }
    final long deltaZ = Math.abs(aZ - bZ);
    final long aId = getIdWithoutZoom(a);
    final long bId = getIdWithoutZoom(b);
    final long x, y;
    final int prio;

    if (aZ < bZ) {
      x = aId;
      y = bId >>> DIMENSION * deltaZ;
      prio = 1;
    } else {
      x = aId >>> DIMENSION * deltaZ;;
      y = bId;
      prio = -1;
    }
    final int r = Long.compare(x, y);
    return (r == 0) ? prio : r;
  };

  public static OSHDBBoundingBox WORLD = new OSHDBBoundingBox(-180.0, -90.0, 180.0, 180.0);

  private static class DFIterator implements Iterator<Long> {

    private final OSHDBBoundingBox search;
    private final int maxZoom;
    private final State[] states;

    private boolean hasNext = true;
    private long next = 0;

    private int z = 1;

    public DFIterator(OSHDBBoundingBox search, int maxZoom, long space) {
      this.search = search;
      this.maxZoom = maxZoom;

      this.states = new State[maxZoom + 1];
      for (int z = 0; z < states.length; z++) {
        State s = new State();
        final long width = space / (long) Math.pow(2, z);
        s.width = width;
        s.bbox = new OSHDBBoundingBox(0L, 0L, width, width);
        states[z] = s;
      }

    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public Long next() {
      long ret = next;
      getNext();
      return ret;
    }

    private void getNext() {
      while (z > 0) {
        final State state = states[z];
        final int zoom = z;
        final long id = state.id;

        if (state.i > 3) {
          state.i = 0;
          state.complete = false;
          z--;
          continue;
        }

        final OSHDBBoundingBox.OVERLAP overlap;
        if (states[z - 1].complete) {
          overlap = OVERLAP.A_COMPLETE_IN_B;
        } else {
          OSHDBBoundingBox parentBBox = states[z - 1].bbox;
          switch (state.i) {
            case 0:
              state.bbox = new OSHDBBoundingBox(parentBBox.getMinLonLong(), parentBBox.getMinLatLong(),
                  parentBBox.getMinLonLong() + state.width, parentBBox.getMinLatLong() + state.width);
              break;
            case 1:
              state.bbox = new OSHDBBoundingBox(parentBBox.getMinLonLong() + state.width, parentBBox.getMinLatLong(),
                  parentBBox.getMinLonLong() + 2 * state.width, parentBBox.getMinLatLong() + state.width);
              break;
            case 2:
              state.bbox = new OSHDBBoundingBox(parentBBox.getMinLonLong(), parentBBox.getMinLatLong() + state.width,
                  parentBBox.getMinLonLong() + state.width, parentBBox.getMinLatLong() + 2 * state.width);
              break;
            case 3:
              state.bbox = new OSHDBBoundingBox(parentBBox.getMinLonLong() + state.width, parentBBox.getMinLatLong() + state.width,
                  parentBBox.getMinLonLong() + 2 * state.width, parentBBox.getMinLatLong() + 2 * state.width);
          }
          overlap = OSHDBBoundingBox.overlap(state.bbox, search);
        }

        if (overlap == OVERLAP.A_COMPLETE_IN_B) {
          state.complete = true;
        }

        if (z < maxZoom && overlap != OVERLAP.NONE) {
          z++;
          states[z].id = state.id << 2;
        }

        state.i += 1;
        state.id += 1;

        if (overlap != OVERLAP.NONE) {
          hasNext = true;
          next = addZoomToId(id, zoom);
          return;
        }
      }
      hasNext = false;
      next = -1;
    }

    private static class State {

      private int i = 0;
      private long id;
      private OSHDBBoundingBox bbox;
      private long width;
      private boolean complete = false;
    }
  }
}
