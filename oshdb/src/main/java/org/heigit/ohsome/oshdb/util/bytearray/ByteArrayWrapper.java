package org.heigit.ohsome.oshdb.util.bytearray;

import java.util.Arrays;

/**
 * An wrapper class around an byte array, offering various read methods.
 */
public class ByteArrayWrapper {

  protected final byte[] buffer;
  private final int offset;
  private final int bufferSize;
  private int bufferPos;

  public static ByteArrayWrapper newInstance(final byte[] buffer) {
    return newInstance(buffer, 0, buffer.length);
  }

  public static ByteArrayWrapper newInstance(final byte[] buffer, final int offset, final int len) {
    return new ByteArrayWrapper(buffer, offset, len);
  }

  /**
   * Constructor for ByteArrayWrapper.
   *
   * @param buffer The buffer to be wrapped
   * @param offset The offset within the buffer
   * @param len The length of bytes which should be included
   */
  public ByteArrayWrapper(final byte[] buffer, final int offset, final int len) {
    this.buffer = buffer;
    this.offset = offset;
    bufferSize = offset + len;
    bufferPos = offset;
  }

  public int getPos() {
    return bufferPos;
  }

  public void reset() {
    bufferPos = offset;
  }

  public void skipTo(int pos) {
    bufferPos = pos;
  }

  public void seek(int pos) {
    bufferPos = pos;
  }

  public int hasLeft() {
    return bufferSize - bufferPos;
  }

  /**
   * Read an {@code sint32} field value from the stream.
   */
  public int readS32() {
    return decodeZigZag32(readU32());
  }

  public int readS32Delta(int last) {
    return readS32() + last;
  }

  /**
   * Read a {@code uint32} field value from the stream.
   */
  public int readU32() {
    int b = buffer[bufferPos++];
    if (b >= 0) {
      return b;
    }

    int value = b & 0x7F;
    var i = 7;
    while (((b = buffer[bufferPos++]) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | b << i;
  }

  /**
   * Read an {@code sint64} field value from the stream.
   */
  public long readS64() {
    return decodeZigZag64(readU64());
  }

  public long readS64Delta(long last) {
    return readS64() + last;
  }

  /**
   * Read a {@code uint64} field value from the stream.
   */
  public long readU64() {
    long b = buffer[bufferPos++];
    if (b >= 0) {
      return b;
    }
    long value = b & 0x7F;
    var i = 7;
    while (((b = buffer[bufferPos++]) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
    }
    return value | b << i;
  }

  public long readU64Delta(long last) {
    return readU64() + last;
  }

  /**
   * Read one byte from the input.
   *
   */
  public byte readRawByte() {
    return buffer[bufferPos++];
  }

  /**
   * Read bytes from input.
   *
   * @param size Number of bytes to read.
   */
  public byte[] readByteArray(int size) {
    if (size <= this.bufferSize - this.bufferPos && size > 0) {
      byte[] result = Arrays.copyOfRange(this.buffer, this.bufferPos, this.bufferPos + size);
      this.bufferPos += size;
      return result;
    }
    return new byte[0];
  }

  /**
   * Decode a ZigZag-encoded 32-bit value. ZigZag encodes signed integers into values that can be
   * efficiently encoded with varint. (Otherwise, negative values must be sign-extended to 64 bits
   * to be varint encoded, thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 32-bit integer, stored in a signed int because Java has no explicit
   *        unsigned support.
   * @return A signed 32-bit integer.
   */
  private static int decodeZigZag32(final int n) {
    return n >>> 1 ^ -(n & 1);
  }

  /**
   * Decode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into values that can be
   * efficiently encoded with varint. (Otherwise, negative values must be sign-extended to 64 bits
   * to be varint encoded, thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 64-bit integer, stored in a signed int because Java has no explicit
   *        unsigned support.
   * @return A signed 64-bit integer.
   */
  private static long decodeZigZag64(final long n) {
    return n >>> 1 ^ -(n & 1);
  }
}
