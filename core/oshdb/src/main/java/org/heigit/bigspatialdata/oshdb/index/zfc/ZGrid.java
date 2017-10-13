package org.heigit.bigspatialdata.oshdb.index.zfc;

import java.util.Comparator;
import java.util.Iterator;

import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.util.LongBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.LongBoundingBox.OVERLAP;

public class ZGrid {

  private static final int DIMENSION = 2;
  private static long ZOOM_FACTOR = 1L << 56;
  private static long ID_MASK = 0x00FFFFFFFFFFFFFFL;

  private final long space;// = (long) (360.0 * OSMNode.GEOM_PRECISION_TO_LONG);
  private final int maxZoom;

  public ZGrid(int maxZoom, long space) {
    if (maxZoom < 1)
      throw new IllegalArgumentException("maxZoom must be >= 1 but is " + maxZoom);
    // maxZoom so that 8 Bit ZoomLevel + maxZoom Bits per Dimension(2) = 64 Bit
    // (Long)
    if (maxZoom > 28)
      throw new IllegalArgumentException("maxZoom must not be > 28 but is " + maxZoom);
    this.maxZoom = maxZoom;
    this.space = space;
  }

  public long getIdSingleZIdWithZoom(long[] lon, long[] lat) {
    final long[] x = new long[2];
    final long[] y = new long[2];

    int zoom = Math.min(optimalZoom(lon[1] - lon[0]), optimalZoom(lat[1] - lat[0]));
    long zoomPow = (long) Math.pow(2, zoom);

    while (zoom > 0) {
      long cellWidth = space / zoomPow;
      x[0] = lon[0] / cellWidth;
      x[1] = lon[1] / cellWidth;
      y[0] = lat[0] / cellWidth;
      y[1] = lat[1] / cellWidth;

      if (x[0] == x[1] && y[0] == y[1])
        break;
      zoom -= 1;
      zoomPow >>>= 1;
    }

    if (zoom == 0) {
      return 0;
    }

    return morton(x[0], y[0]) + (zoom * ZOOM_FACTOR);
  }

  public static int getZoom(long zId) {
    return (int) (zId / ZOOM_FACTOR);
  }

  public static long getIdWithoutZoom(long zId) {
    return zId & ID_MASK;
  }
  
  public Iterable<Long> iterableDF(LongBoundingBox search) {
    final ZGrid zGrid = this;
    return new Iterable<Long>() {
      final ZGrid grid = zGrid;

      @Override
      public Iterator<Long> iterator() {
        return grid.iteratorDF(search);
      }
    };
  }

  public Iterator<Long> iteratorDF(LongBoundingBox search) {
    return new DFIterator(search, maxZoom,space);
  }

  public LongBoundingBox getBoundingBox(long zId) {
    final long id = getIdWithoutZoom(zId);
    final int zoom = getZoom(zId);
    final long cellWidth = space / (long) Math.pow(2, zoom);

    long[] xy = getXY(id);

    System.out.printf("x:%d, y:%d%n", xy[0], xy[1]);

    final long[] lon = new long[2];
    lon[0] = xy[0] * cellWidth;
    lon[1] = lon[0] + cellWidth - 1;
    final long[] lat = new long[2];
    lat[0] = xy[1] * cellWidth;
    lat[1] = lat[0] + cellWidth - 1;

    return new LongBoundingBox(lon, lat);
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

  

  public void bla(LongBoundingBox bbox) {
    final long[][] b = new long[maxZoom][4];
    long zoomPow = 1;
    for (int z = 1; z < maxZoom; z++) {
      long cellWidth = space / zoomPow;
      b[z][0] = bbox.minLon / cellWidth;
      b[z][1] = bbox.maxLon / cellWidth;
      b[z][2] = bbox.minLat / cellWidth;
      b[z][3] = bbox.maxLat / cellWidth;
      zoomPow <<= 1;
    }
  }

  public static final Comparator<Long> ORDER_DFS = (a, b) -> {
    final long aZ = getZoom(a);
    final long bZ = getZoom(b);
    if (aZ == bZ)
      return Long.compare(a, b);
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

  private static class DFIterator implements Iterator<Long> {

    private static class State {
      private int i = 0;
      private long id;
      private LongBoundingBox bbox;
      private long width;
      private boolean complete = false;
    }

    private final LongBoundingBox search;
    private final int maxZoom;
    private final State[] states;

    private boolean hasNext = true;
    private long next = 0;

    private int z = 1;

    public DFIterator(LongBoundingBox search, int maxZoom, long space) {
      this.search = search;
      this.maxZoom = maxZoom;

      this.states = new State[maxZoom + 1];
      for (int z = 0; z < states.length; z++) {
        State s = new State();
        final long width = space / (long) Math.pow(2, z);
        s.width = width;
        s.bbox = new LongBoundingBox(0, width, 0, width);
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

        final LongBoundingBox.OVERLAP overlap;
        if (states[z - 1].complete) {
          overlap = OVERLAP.A_COMPLETE_IN_B;
        } else {
          LongBoundingBox parentBBox = states[z - 1].bbox;
          switch (state.i) {
          case 0:
            state.bbox = new LongBoundingBox(parentBBox.minLon, parentBBox.minLon + state.width, parentBBox.minLat,
                parentBBox.minLat + state.width);
            break;
          case 1:
            state.bbox = new LongBoundingBox(parentBBox.minLon + state.width, parentBBox.minLon + 2 * state.width,
                parentBBox.minLat, parentBBox.minLat + state.width);
            break;
          case 2:
            state.bbox = new LongBoundingBox(parentBBox.minLon, parentBBox.minLon + state.width,
                parentBBox.minLat + state.width, parentBBox.minLat + 2 * state.width);
            break;
          case 3:
            state.bbox = new LongBoundingBox(parentBBox.minLon + state.width, parentBBox.minLon + 2 * state.width,
                parentBBox.minLat + state.width, parentBBox.minLat + 2 * state.width);
          }
          overlap = LongBoundingBox.overlap(state.bbox, search);
        }

        if (overlap == OVERLAP.A_COMPLETE_IN_B)
          state.complete = true;

        if (z < maxZoom && overlap != OVERLAP.NONE) {
          z++;
          states[z].id = state.id << 2;
        }

        state.i += 1;
        state.id += 1;

        if (overlap != OVERLAP.NONE) {
          hasNext = true;
          next = id + ZOOM_FACTOR * zoom;
          return;
        }
      }
      hasNext = false;
      next = -1;
    }

    private static int overlap(LongBoundingBox bbox, LongBoundingBox test) {
      if (test.minLon >= bbox.maxLon)
        return 0;
      if (test.maxLon <= bbox.minLon)
        return 0;
      if (test.minLat >= bbox.maxLat)
        return 0;
      if (test.maxLat <= bbox.minLat)
        return 0; // no overlap

      // fit bbox in test
      if (bbox.minLon >= test.minLon && bbox.maxLon <= test.maxLon && bbox.minLat >= test.minLat
          && bbox.maxLat <= test.maxLat)
        return 3;
      // fit test in bbox
      if (test.minLon >= bbox.minLon && test.maxLon <= bbox.maxLon && test.minLat >= bbox.minLat
          && test.maxLat <= bbox.maxLat)
        return 2;
      return 1;
    }

  }
  
  public static void main(String[] args){
    final int zoom = 20;
    final long space = (long) (360.0 * OSMNode.GEOM_PRECISION_TO_LONG);
    final long zoompow = (long) Math.pow(2, zoom);
    final long cellWidth = space / zoompow;
    ZGrid g = new ZGrid(zoom, space);
    // new LongBoundingBox(1,3,2,4))//
    g.iterableDF(new LongBoundingBox(86860589L, 86860589L + cellWidth, 494139909L, 494139909L + 100)).forEach(zId2 -> {
      System.out.printf("z:%2d - %d%n", g.getZoom(zId2), g.getIdWithoutZoom(zId2));
    });
  }

  /*
  public static void main(String[] args) {
    final int zoom = 20;
    final long zoompow = (long) Math.pow(2, zoom);
    final long cellWidth = space / zoompow;

    // 49,4139909, 8,6860589
    final long[] lon = new long[] { 2 * cellWidth, 0 };
    lon[1] = lon[0] + 1;
    final long[] lat = new long[] { 2 * cellWidth, 0 };
    lat[1] = lat[0] + 1;

    ZGrid zGrid = new ZGrid(20);

    long zId = zGrid.getIdSingleZIdWithZoom(lon, lat);

    System.out.println(zId);
    System.out.println(zGrid.getZoom(zId));
    System.out.println(zGrid.getIdWithoutZoom(zId));
    LongBoundingBox bbox = zGrid.getBoundingBox(zId);

    long zIdBBox = zGrid.getIdSingleZIdWithZoom(bbox.getLon(), bbox.getLat());
    System.out.printf("%d = %d%n", zId, zIdBBox);

    long x1 = 0;// 0xFFFFFFFL;
    long y1 = 0xFFFFFFFL;

    long morton = morton(x1, y1);
    System.out.printf("-%64s-%n", Long.toBinaryString(x1));
    System.out.printf("-%64s-%n", Long.toBinaryString(y1));
    System.out.printf("-%64s-%n", Long.toBinaryString(morton));
    System.out.printf("-%64s-%n", Long.toBinaryString(ZOOM_FACTOR * 255));

    long minCellWidth = space / 0xFFFFFFFL;
    System.out.printf("-%64s-%n", Long.toBinaryString(minCellWidth));
    long minZoomPow = space / minCellWidth;
    System.out.printf("-%64s-%n", Long.toBinaryString(minZoomPow));
    System.out.println(63 - Long.numberOfLeadingZeros(minZoomPow));

    long mx = 0, my = 0;
    for (int i = 0; i < 5; i++) {
      long[] xy = getXY(i);
      System.out.println(i + ": " + Arrays.toString(xy));
    }

    long minX = 1, minY = 2;
    long maxX = 3, maxY = 4;

    // 9, 11, 12, 13, 14, 15, 33, 36, 37
    for (long id = morton(minX, minY); true; id++) {
      long[] xy = getXY(id);
      if (xy[0] < minX || xy[0] > maxX || xy[1] < minY || xy[1] > maxY)
        continue;
      System.out.printf("%d - %s%n", id, Arrays.toString(xy));
      if (xy[0] == maxX && xy[1] == maxY)
        break;
    }

    ZGrid g = new ZGrid(20);
    // new LongBoundingBox(1,3,2,4))//
    g.iterableDF(new LongBoundingBox(86860589L, 86860589L + cellWidth, 494139909L, 494139909L + 100)).forEach(zId2 -> {
      System.out.printf("z:%2d - %d%n", getZoom(zId2), getIdWithoutZoom(zId2));
    });

  }
*/
}
