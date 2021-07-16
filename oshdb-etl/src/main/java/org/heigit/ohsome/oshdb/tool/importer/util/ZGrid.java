package org.heigit.ohsome.oshdb.tool.importer.util;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;

import java.util.Comparator;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;

@SuppressWarnings("checkstyle:abbreviationAsWordInName")
public class ZGrid {

  private static final int DIMENSION = 2;
  private static long ZOOM_FACTOR = 1L << 56;
  private static long ID_MASK = 0x00FFFFFFFFFFFFFFL;

  private static final long space = (long) (360.0 * OSMCoordinates.GEOM_PRECISION_TO_LONG);
  private final int maxZoom;
  private static final OSHDBBoundingBox zeroBoundingBox = bboxWgs84Coordinates(0.0, 0.0, 0.0, 0.0);

  /**
   * Creates a {@code ZGrid} index based on maximal Zoom.
   */
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

  public long getIdSingleZidWithZoom(long lon, long lat) {
    return getIdSingleZidWithZoom(lon, lon, lat, lat);
  }

  public long getIdSingleZidWithZoom(long[] lon, long[] lat) {
    return getIdSingleZidWithZoom(lon[0], lon[1], lat[0], lat[1]);
  }

  public long getIdSingleZidWithZoom(long minLon, long maxLon, long minLat, long maxLat) {
    final long[] x = new long[2];
    final long[] y = new long[2];

    minLon = normalizeLon(minLon);
    maxLon = normalizeLon(maxLon);
    minLat = normalizeLat(minLat);
    maxLat = normalizeLat(maxLat);

    if (!validateLon(minLon) || !validateLon(maxLon) || !validateLat(minLat)
        || !validateLat(maxLat)) {
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

  public static int getZoom(long zid) {
    return (int) (zid / ZOOM_FACTOR);
  }

  public static long addZoomToId(long id, int zoom) {
    return id + zoom * ZOOM_FACTOR;
  }

  public static long getIdWithoutZoom(long zid) {
    return zid & ID_MASK;
  }

  public static long getParent(long zid, int parentZoom) {
    final int zoom = getZoom(zid);
    if (zoom < parentZoom) {
      throw new IllegalArgumentException("zoom of id already lesser than parentZoom (zoom:" + zoom
          + " parentZoom:" + parentZoom + ")");
    }
    if (zoom == parentZoom) {
      return zid;
    }
    final long diff = (long) zoom - parentZoom;
    final long id = getIdWithoutZoom(zid) >>> diff * 2;

    return addZoomToId(id, parentZoom);
  }

  public static long getParent(long zid) {
    final int zoom = getZoom(zid);
    if (zoom > 0) {
      final long parentId = getIdWithoutZoom(zid) >>> 2;
      return addZoomToId(parentId, zoom - 1);
    }
    return 0;
  }

  public static OSHDBBoundingBox getBoundingBox(long zid) {
    if (zid < 0) {
      return zeroBoundingBox;
    }
    final long id = getIdWithoutZoom(zid);
    final int zoom = getZoom(zid);
    final long cellWidth = space / (long) Math.pow(2, zoom);

    long[] xy = getXy(id);

    final int minLon = (int) denormalizeLon(xy[0] * cellWidth);
    final int maxLon = (int) (minLon + cellWidth - 1);
    final int minLat = (int) denormalizeLat(xy[1] * cellWidth);
    final int maxLat = (int) (minLat + cellWidth - 1);

    return OSHDBBoundingBox.bboxOSMCoordinates(minLon, minLat, maxLon, maxLat);
  }

  private static long[] getXy(long id) {
    long[] xy = new long[2];
    for (long mask = 1, offset = 0;
        id >= 1 << offset && mask < Integer.MAX_VALUE;
        mask <<= 1) {
      xy[0] |= id >> offset++ & mask;
      xy[1] |= id >> offset & mask;
    }
    return xy;
  }

  private int optimalZoom(long delta) {
    if (delta == 0) {
      return maxZoom;
    }
    return Math.min(63 - Long.numberOfLeadingZeros(space / delta), maxZoom);
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
    return lon + OSMCoordinates.toOSM(180.0);
  }

  private static long denormalizeLon(long lon) {
    return lon - OSMCoordinates.toOSM(180.0);
  }

  private static long normalizeLat(long lat) {
    return lat + OSMCoordinates.toOSM(90.0);
  }

  private static long denormalizeLat(long lat) {
    return lat - OSMCoordinates.toOSM(90.0);
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
    if (a == -1) {
      return b == -1 ? 0 : -1;
    }
    if (b == -1) {
      return 1;
    }

    final long aZ = getZoom(a);
    final long bZ = getZoom(b);
    if (aZ == bZ) {
      return Long.compare(a, b);
    }
    final long deltaZ = Math.abs(aZ - bZ);
    final long aId = getIdWithoutZoom(a);
    final long bId = getIdWithoutZoom(b);
    final long x;
    final long y;
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
    return r == 0 ? prio : r;
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
    final long x;
    final long y;
    final int prio;

    if (aZ < bZ) {
      x = aId;
      y = bId >>> DIMENSION * deltaZ;
      prio = 1;
    } else {
      x = aId >>> DIMENSION * deltaZ;
      y = bId;
      prio = -1;
    }
    final int r = Long.compare(x, y);
    return r == 0 ? prio : r;
  };

}
