package org.heigit.bigspatialdata.oshdb.util.bitset;

import java.util.HashMap;
import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

public class LongRoaringBitmap {

  /** Number of bits allocated to a value in an index */
  private static final int VALUE_BITS = 31; // full positiv integer range, Integer.MAX_VALUE = ,
                                            // 2^31-1
  /** Mask for extracting values */
  private static final long VALUE_MASK = (1l << VALUE_BITS) - 1;


  private final Map<Long, RoaringBitmap> bitmapSets = new HashMap<Long, RoaringBitmap>();


  /**
   * Get set index by long index
   * 
   * @param index Long index
   * @return Index of a bit set in the inner map
   */
  static private long getSetIndex(final long index) {
    return index >> VALUE_BITS;
  }

  /**
   * Get index of a value in a bit set (bits 0-19)
   * 
   * @param index Long index
   * @return Index of a value in a bit set
   */
  static private int getPos(final long index) {
    return (int) (index & VALUE_MASK);
  }


  private RoaringBitmap bitmap(final long index) {
    final Long iIndex = getSetIndex(index);
    RoaringBitmap bitmap = bitmapSets.get(iIndex);
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      bitmapSets.put(iIndex, bitmap);
    }
    return bitmap;
  }



  /**
   * Set a given value for a given index
   * 
   * @param index Long index
   * @param value Value to set
   */
  public void set(final long index, final boolean value) {
    if (value)
      bitmap(index).add(getPos(index));
    else { // if value shall be cleared, check first if given partition exists
      final RoaringBitmap bitmap = bitmapSets.get(getSetIndex(index));
      if (bitmap != null)
        bitmap.remove(getPos(index));
    }
  }

  /**
   * Get a value for a given index
   * 
   * @param index Long index
   * @return Value associated with a given index
   */
  public boolean get(final long index) {
    final RoaringBitmap bitmap = bitmapSets.get(getSetIndex(index));
    return bitmap != null && bitmap.contains(getPos(index));
  }

  public void set(final long index) {
    set(index, true);
  }

  public void add(final long index) {
    set(index, true);
  }

  public void remove(final long index) {
    set(index, false);
  }
  
}
