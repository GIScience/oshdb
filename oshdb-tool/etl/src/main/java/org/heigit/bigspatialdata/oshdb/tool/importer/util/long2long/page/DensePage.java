package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page;

import java.io.IOException;
import java.util.Arrays;

import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

public class DensePage implements Page {
  private final long[] cellIds;

  public DensePage(long[] cellIds) {
    this.cellIds = cellIds;
  }

  @Override
  public long get(int offset) {
    return cellIds[offset];
  }

  @Override
  public int weigh() {
    return 8 * cellIds.length;
  }
  
  public static DensePage of(RoaringBitmap bitmap, ByteArrayWrapper data, int pageSize){
     DensePage page = new DensePage(new long[pageSize]);
     
     Arrays.fill(page.cellIds, -1);
     
     
     bitmap.forEach(new IntConsumer() {
       private long lastValue = 0;
       @Override
       public void accept(int bit) {
           try {
            page.cellIds[bit] = data.readSInt64()+lastValue;
          } catch (IOException e) {
            // TODO throw unchecked exception
          }
         lastValue = page.cellIds[bit];
        
       }
     });
     
     return page;
  }
}
