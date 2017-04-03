package org.heigit.bigspatialdata.hosmdb.grid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMWay;

public class HOSMCellWays extends HOSMCell implements Iterable<HOSMWay>, Serializable {

  private static final long serialVersionUID = 1L;

  public static HOSMCellWays compact(final long id, final int level, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude, final List<HOSMWay> list)
      throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    int offset = 0;
    for (int i = 0; i < index.length; i++) {
      final HOSMWay c = list.get(i).rebase(baseId, baseTimestamp, baseLongitude, baseLatitude);
      final byte[] buffer = c.getData();
      index[i] = offset;
      out.write(buffer);
      offset += buffer.length;
    }
    final byte[] data = out.toByteArray();
    
    return new HOSMCellWays(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  public HOSMCellWays(final long id, final int level, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude, final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }
  
  @Override
  public Iterator<HOSMWay> iterator() {
    return new Iterator<HOSMWay>() {
      private int pos = 0;

      @Override
      public HOSMWay next() {
        int offset = index[pos];
        int length = ((pos < index.length - 1) ? index[pos + 1] : data.length) - offset;
        pos++;
        try {
          return HOSMWay.instance(data, offset, length, baseId, baseTimestamp, baseLongitude,
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

}
