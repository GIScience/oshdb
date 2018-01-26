package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayWrapper;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import com.google.common.cache.CacheLoader;

import it.unimi.dsi.fastutil.ints.Int2LongAVLTreeMap;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;


public class PageLoader extends CacheLoader<Integer, Page> {

  private static final Page empty = new EmptyPage();
  
  private final Map<Integer, PageLocation> pageIndex;
  private final RandomAccessFile rafPages;
  private final BiFunction<byte[],Integer, byte[]> decompress;
  private final int pageSizePower;
  private final int pageSize;

  public PageLoader(String pageIndex, RandomAccessFile rafPages, BiFunction<byte[],Integer, byte[]> decompress)
      throws IOException {
    this.rafPages = rafPages;
    this.decompress = decompress;
    this.pageIndex = new HashMap<>();
    try (BufferedInputStream input = new BufferedInputStream(new FileInputStream(pageIndex));
        DataInputStream dataInput = new DataInputStream(input)) {

      this.pageSizePower = dataInput.readInt();
      this.pageSize = (int) Math.pow(2, pageSizePower);
      
      try {
        while (true) {
          final int pageNumber = dataInput.readInt();
          final long offset = dataInput.readLong();
          final int size = dataInput.readInt();
          final int rawSize = dataInput.readInt();
          this.pageIndex.put(Integer.valueOf(pageNumber), new PageLocation(offset, size,rawSize));
        }
      } catch (EOFException e) {
      }
    }
  }

  public int getPageSizePower() {
    return pageSizePower;
  }

  @Override
  public Page load(Integer key) throws Exception {
  //  System.out.println("Load Page "+key);
    PageLocation loc = pageIndex.get(key);
    if (loc == null)
      return empty;

    rafPages.seek(loc.offset);
    byte[] bytes = new byte[loc.size];
    rafPages.readFully(bytes, 0, bytes.length);
    bytes = decompress.apply(bytes,loc.rawSize);

    FastByteArrayInputStream input = new FastByteArrayInputStream(bytes);
    DataInputStream in = new DataInputStream(input);
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.deserialize(in);
    
    
    ByteArrayWrapper wrapper = ByteArrayWrapper.newInstance(bytes, (int) input.position(), input.available());
    
    if(bitmap.getLongCardinality() > (pageSize/2)){
      long[] pageContent = new long[pageSize];
      
      Arrays.fill(pageContent, -1);
          
      bitmap.forEach(new IntConsumer() {
        private long lastValue = 0;
        @Override
        public void accept(int bit) {
          try {
            pageContent[bit] = wrapper.readSInt64()+lastValue;
            lastValue = pageContent[bit];
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    
      return new DensePage(pageContent);
    }else{
      Int2LongAVLTreeMap map = new Int2LongAVLTreeMap();
      map.defaultReturnValue(-1);
      
      bitmap.forEach(new IntConsumer() {
        private long value = 0;
        @Override
        public void accept(int bit) {
          try {
            value = wrapper.readSInt64()+value; 
            map.put(bit, value);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
      
      return new SparsePage(map);
    }
  }
}
