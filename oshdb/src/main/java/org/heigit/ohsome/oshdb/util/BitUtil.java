package org.heigit.ohsome.oshdb.util;

/**
 * <a href=
 * "https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/util/BitUtil.java"
 * >org.apache.lucence.util.BitUtil</a>
 *
 *
 */
public class BitUtil {
  private BitUtil() {} // no instance

  public static boolean checkBit(int flags, int mask) {
    return (flags & mask) != 0;
  }

  // magic numbers for bit interleaving
  private static final long MAGIC0 = 0x5555555555555555L;
  private static final long MAGIC1 = 0x3333333333333333L;
  private static final long MAGIC2 = 0x0F0F0F0F0F0F0F0FL;
  private static final long MAGIC3 = 0x00FF00FF00FF00FFL;
  private static final long MAGIC4 = 0x0000FFFF0000FFFFL;
  private static final long MAGIC5 = 0x00000000FFFFFFFFL;
  private static final long MAGIC6 = 0xAAAAAAAAAAAAAAAAL;

  // shift values for bit interleaving
  private static final long SHIFT0 = 1;
  private static final long SHIFT1 = 2;
  private static final long SHIFT2 = 4;
  private static final long SHIFT3 = 8;
  private static final long SHIFT4 = 16;

  /**
   * Interleaves the first 32 bits of each long value
   *
   * <p>Adapted from: http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
   */
  public static long interleave(int even, int odd) {
    long v1 = 0x00000000FFFFFFFFL & even;
    v1 = (v1 | (v1 << SHIFT4)) & MAGIC4;
    v1 = (v1 | (v1 << SHIFT3)) & MAGIC3;
    v1 = (v1 | (v1 << SHIFT2)) & MAGIC2;
    v1 = (v1 | (v1 << SHIFT1)) & MAGIC1;
    v1 = (v1 | (v1 << SHIFT0)) & MAGIC0;

    long v2 = 0x00000000FFFFFFFFL & odd;
    v2 = (v2 | (v2 << SHIFT4)) & MAGIC4;
    v2 = (v2 | (v2 << SHIFT3)) & MAGIC3;
    v2 = (v2 | (v2 << SHIFT2)) & MAGIC2;
    v2 = (v2 | (v2 << SHIFT1)) & MAGIC1;
    v2 = (v2 | (v2 << SHIFT0)) & MAGIC0;

    return (v2 << 1) | v1;
  }

  /** Extract just the even-bits value as a long from the bit-interleaved value. */
  public static long deinterleave(long b) {
    b &= MAGIC0;
    b = (b ^ (b >>> SHIFT0)) & MAGIC1;
    b = (b ^ (b >>> SHIFT1)) & MAGIC2;
    b = (b ^ (b >>> SHIFT2)) & MAGIC3;
    b = (b ^ (b >>> SHIFT3)) & MAGIC4;
    b = (b ^ (b >>> SHIFT4)) & MAGIC5;
    return b;
  }

  /** flip flops odd with even bits. */
  public static long flipFlop(final long b) {
    return ((b & MAGIC6) >>> 1) | ((b & MAGIC0) << 1);
  }

  /** Same as {@link #zigZagEncode(long)} but on integers. */
  public static int zigZagEncode(int i) {
    return (i >> 31) ^ (i << 1);
  }

  /**
   * <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">Zig-zag</a> encode
   * the provided long. Assuming the input is a signed long whose absolute value can be stored on
   * <code>n</code> bits, the returned value will be an unsigned long that can be stored on <code>
   * n+1</code> bits.
   */
  public static long zigZagEncode(long l) {
    return (l >> 63) ^ (l << 1);
  }

  /** Decode an int previously encoded with {@link #zigZagEncode(int)}. */
  public static int zigZagDecode(int i) {
    return ((i >>> 1) ^ -(i & 1));
  }

  /** Decode a long previously encoded with {@link #zigZagEncode(long)}. */
  public static long zigZagDecode(long l) {
    return ((l >>> 1) ^ -(l & 1));
  }
}
