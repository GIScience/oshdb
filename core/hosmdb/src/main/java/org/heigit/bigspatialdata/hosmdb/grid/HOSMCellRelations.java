package org.heigit.bigspatialdata.hosmdb.grid;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.heigit.bigspatialdata.hosmdb.osh.HOSMRelation;

public class  HOSMCellRelations extends HOSMCell implements Iterable<HOSMRelation>, Serializable {


  private static final long serialVersionUID = 1L;
  
  public static HOSMCellRelations compact(final long id, final int level, final long baseId,
      final long baseTimestamp, final long baseLongitude, final long baseLatitude,
      final List<HOSMRelation> list) throws IOException {

    int offset = 0;

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final int[] index = new int[list.size()];
    // TODO user iterator!!
    for (int i = 0; i < index.length; i++) {
      final HOSMRelation c = list.get(i).compact(baseId, baseTimestamp, baseLongitude, baseLatitude);
      final byte[] buffer = c.getData();
      index[i] = offset;
      out.write(buffer);
      offset += buffer.length;
    }
    final byte[] data = out.toByteArray();
    return new HOSMCellRelations(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index,
        data);
  }

  private HOSMCellRelations(final long id, final int level, final long baseId, final long baseTimestamp,
      final long baseLongitude, final long baseLatitude, final int[] index, final byte[] data) {
    super(id, level, baseId, baseTimestamp, baseLongitude, baseLatitude, index, data);
  }

  @Override
  public Iterator<HOSMRelation> iterator() {
    return new Iterator<HOSMRelation>() {
      private int pos = 0;

      @Override
      public HOSMRelation next() {
        int offset = index[pos];
        int length = ((pos < index.length - 1) ? index[pos + 1] : data.length) - offset;
        pos++;
        try {
          return HOSMRelation.instance(data, offset, length, baseId, baseTimestamp, baseLongitude,
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
