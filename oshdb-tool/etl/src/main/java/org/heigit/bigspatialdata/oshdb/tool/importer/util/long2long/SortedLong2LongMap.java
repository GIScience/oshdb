package org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page.Page;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.long2long.page.PageLoader;
import org.heigit.bigspatialdata.oshdb.util.byteArray.ByteArrayOutputWrapper;
import org.roaringbitmap.RoaringBitmap;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

public class SortedLong2LongMap implements LongToLongMap {
  public static class Sink implements Closeable {
    private final int pageSizePower;
    private final int pageSize;
    private final long pageOffsetMask;

    private ByteArrayOutputWrapper output = new ByteArrayOutputWrapper();
    private RoaringBitmap bitmap = new RoaringBitmap();

    private RandomAccessFile rafPages;
    private RandomAccessFile rafPageIndex;
    private Function<byte[], byte[]> compress;

    private long lastId = -1;
    private long lastValue = 0;
    private int lastPageNumber = -1;

    public Sink(Path pathWithoutSuffix, int pageSizePower) throws FileNotFoundException {
      this(pathWithoutSuffix, pageSizePower, in -> in);
    }

    public Sink(Path pathWithoutSuffix, int pageSizePower, Function<byte[], byte[]> compress)
        throws FileNotFoundException {
      rafPages = new RandomAccessFile(pathWithoutSuffix.toString() + ".map", "rw");
      rafPageIndex = new RandomAccessFile(pathWithoutSuffix.toString() + ".idx", "rw");

      this.compress = compress;
      this.pageSizePower = pageSizePower;
      this.pageSize = (int) Math.pow(2, pageSizePower);
      this.pageOffsetMask = pageSize - 1;
    }

    public void put(long id, long value) throws IOException {
      if (id < 0)
        throw new IllegalArgumentException("id must greater than 0 but is " + id);
      if (id <= lastId)
        throw new IllegalArgumentException(
            "id must in strict acsending order lastId was " + lastId + " new id is " + id);

      final int pageNumber = (int) (id / pageOffsetMask);
      final int pageOffset = (int) (id & pageOffsetMask);

      if (pageNumber != lastPageNumber) {
        flushPage();
        lastPageNumber = pageNumber;
      }

      output.writeSInt64(value - lastValue);
      bitmap.add(pageOffset);

      lastId = id;
      lastValue = value;
    }

    public void close() {
      try {
        flushPage();
        rafPages.close();
        rafPageIndex.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    private void flushPage() throws IOException {
      if (lastPageNumber == -1) {
        rafPageIndex.writeInt(pageSizePower);
        return;
      }

      bitmap.runOptimize();
      ByteArrayOutputStream baos = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
      DataOutputStream dos = new DataOutputStream(baos);
      bitmap.serialize(dos);

      dos.write(output.array(), 0, output.length());
      dos.close();

      final byte[] raw = baos.toByteArray();
      final byte[] compressed = compress.apply(raw);
      final long pos = rafPages.getFilePointer();

      rafPages.write(compressed);

      // System.out.printf("id2cell flushPage:%d(%d)
      // size(%d,%d)%n",lastPageNumber,lastId,compressed.length,raw.length);
      rafPageIndex.writeInt(lastPageNumber);
      rafPageIndex.writeLong(pos);
      rafPageIndex.writeInt(compressed.length);
      rafPageIndex.writeInt(raw.length);

      output = new ByteArrayOutputWrapper();
      bitmap.clear();
      lastValue = 0;
    }
  }

  private final int pageSizePower;
  private final int pageSize;
  private final int pageOffsetMask;

  private final LoadingCache<Integer, Page> cache;
  private RandomAccessFile rafPages;

  public SortedLong2LongMap(Path pathWithoutSuffix, long maxMemory) throws IOException {
    this(pathWithoutSuffix, maxMemory, (in, size) -> in);
  }

  public SortedLong2LongMap(Path pathWithoutSuffix, long maxMemory, BiFunction<byte[], Integer, byte[]> comression)
      throws IOException {
    this.rafPages = new RandomAccessFile(pathWithoutSuffix.toString() + ".map", "r");
    PageLoader pageLoader = new PageLoader(pathWithoutSuffix.toString() + ".idx", rafPages, comression);
    this.cache = initCache(maxMemory, pageLoader);

    this.pageSizePower = pageLoader.getPageSizePower();
    this.pageSize = (int) Math.pow(2, pageSizePower);
    this.pageOffsetMask = pageSize - 1;
  }

  public LongSortedSet get(LongSortedSet ids) {
    if (ids.isEmpty())
      return ids;

    try {
      final LongSortedSet result = new LongAVLTreeSet();

      Page page = null;
      int currentPageNumber = -1;

      LongIterator itr = ids.iterator();
      while (itr.hasNext()) {
        long id = itr.nextLong();
        int pageNumber = (int) (id / pageOffsetMask);
        int pageOffset = (int) (id & pageOffsetMask);
        if (currentPageNumber != pageNumber) {
          page = cache.get(pageNumber);
          currentPageNumber = pageNumber;
        }
        
        long cellId = page.get(pageOffset);
        if(cellId >= 0)
          result.add(cellId);
      }
      return result;
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }

  }

  public long get(long id) {
    if (id < 0)
      throw new IllegalArgumentException("id must greater than 0 but is " + id);

    final int pageNumber = (int) (id / pageOffsetMask);
    final int pageOffset = (int) (id & pageOffsetMask);

    try {
      Page page = cache.get(pageNumber);
      final long cellId = page.get(pageOffset);
      return cellId;

    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return -1;
  }

  public void close() {
    try {
      rafPages.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private LoadingCache<Integer, Page> initCache(long maxMemory, CacheLoader<Integer, Page> cacheLoader) {
    return CacheBuilder.newBuilder()
        .maximumWeight(maxMemory)
        .weigher(new Weigher<Integer, Page>() {
          @Override
          public int weigh(Integer arg0, Page page) {
            return page.weigh();
          }
        })
        /*
        .removalListener(new RemovalListener<Integer, Page>() {
          @Override
          public void onRemoval(RemovalNotification<Integer, Page> notification) {
            System.out.println("evict "+ notification.getKey());
            
          }
        })
        */
        .build(cacheLoader);
  }

}
