package org.heigit.ohsome.oshdb.grid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntities;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHWay;

public class GridOSHWays extends GridOSHEntity implements Iterable<OSHWay> {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new {@code GridOSHWays} while rebase/compacting the input ways.
   */
  public static GridOSHWays compact(final long id, final int level, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final List<OSHWay> list) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    int offset = 0;
    for (int i = 0; i < index.length; i++) {
      final OSHWay osh = list.get(i);
      final ByteBuffer buffer = OSHWayImpl.buildRecord(OSHEntities.toList(osh.getVersions()),
          osh.getNodes(), baseId, baseTimestamp, baseLongitude, baseLatitude);
      index[i] = offset;
      out.write(buffer.array(), 0, buffer.remaining());
      offset += buffer.remaining();
    }
    final byte[] data = out.toByteArray();
    return new GridOSHWays(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index,
        data);
  }

  public GridOSHWays(final long id, final int level, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude, final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  @Override
  public Iterable<? extends OSHEntity> getEntities() {
    return this;
  }

  @Override
  public Iterator<OSHWay> iterator() {
    return new Iterator<>() {
      private int pos = 0;

      @Override
      public OSHWay next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        int offset = index[pos];
        int length = (pos < index.length - 1 ? index[pos + 1] : data.length) - offset;
        pos++;
        try {
          return OSHWayImpl.instance(data, offset, length, baseId, baseTimestamp, baseLongitude,
              baseLatitude);
        } catch (IOException e) {
          e.printStackTrace();
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return pos < index.length;
      }
    };
  }

  @Override
  public String toString() {
    return String.format("Grid-Cell of OSHWays %s", super.toString());
  }

}
