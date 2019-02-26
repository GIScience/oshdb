package org.heigit.bigspatialdata.oshdb.util.byteArray;

import java.io.IOException;
import java.io.OutputStream;

public class FastByteArrayOutputStream extends OutputStream {
  /** The array backing the output stream. */
  public static final int DEFAULT_INITIAL_CAPACITY = 16;

  /** The array backing the output stream. */
  public byte[] array;

  /** The number of valid bytes in {@link #array}. */
  public int length;

  /** The current writing position. */
  private int position;

  /**
   * Creates a new array output stream with an initial capacity of {@link #DEFAULT_INITIAL_CAPACITY}
   * bytes.
   */
  public FastByteArrayOutputStream() {
    this(DEFAULT_INITIAL_CAPACITY);
  }

  /**
   * Creates a new array output stream with a given initial capacity.
   *
   * @param initialCapacity the initial length of the backing array.
   */
  public FastByteArrayOutputStream(final int initialCapacity) {
    array = new byte[initialCapacity];
  }

  /**
   * Creates a new array output stream wrapping a given byte array.
   *
   * @param a the byte array to wrap.
   */
  public FastByteArrayOutputStream(final byte[] a) {
    array = a;
  }

  /** Marks this array output stream as empty. */
  public void reset() {
    length = 0;
    position = 0;
  }

  @Override
  public void write(final int b) {
    if (position >= array.length)
      array = grow(array, position + 1, length);
    array[position++] = (byte) b;
    if (length < position)
      length = position;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    if (position + len > array.length)
      array = grow(array, position + len, position);
    System.arraycopy(b, off, array, position, len);
    if (position + len > length)
      length = position += len;
  }

  public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private byte[] grow(final byte[] array, final int length, final int preserve) {
    if (length > array.length) {
      final int newLength = (int) Math.max(Math.min(2L * array.length, MAX_ARRAY_SIZE), length);
      final byte t[] = new byte[newLength];
      System.arraycopy(array, 0, t, 0, preserve);
      return t;
    }
    return array;
  }
}
