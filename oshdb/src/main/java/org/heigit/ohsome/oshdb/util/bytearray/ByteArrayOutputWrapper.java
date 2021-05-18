package org.heigit.ohsome.oshdb.util.bytearray;

import java.io.IOException;

/**
 * A wrapper around {@link OSHDBByteArrayOutputStream} for extending varint encoding methods.
 */
public class ByteArrayOutputWrapper {

  protected final OSHDBByteArrayOutputStream bos;

  public ByteArrayOutputWrapper(int bufferSize) {
    bos = new OSHDBByteArrayOutputStream(bufferSize);
  }

  public ByteArrayOutputWrapper() {
    this(256);
  }

  /**
   * Writes a unsigned 32bit integer varint encoded to the underlying Stream.
   *
   * @param value 32bit integer to encode.
   */
  public void writeU32(int value) {
    while ((value & ~0x7F) != 0) {
      bos.write(value & 0x7F | 0x80);
      value >>>= 7;
    }
    bos.write(value);
  }

  /**
   * Writes a signed 32bit integer varint encoded to the underlying Stream.
   *
   * @param value 32bit integer to encode.
   */
  public void writeS32(int value) {
    writeU32(encodeZigZag32(value));
  }

  public int writeS32Delta(int value, int last) {
    writeS32(value - last);
    return value;
  }

  /**
   * Writes a unsigned 64bit integer varint encoded to the underlying Stream.
   *
   * @param value 64bit integer to encode.
   */
  public void writeU64(long value) {
    while (((int) value & ~0x7FL) != 0) {
      bos.write((int) value & 0x7F | 0x80);
      value >>>= 7;
    }
    bos.write((int) value);
  }

  /**
   * Write a delta encoded value to the stream.
   *
   * @param value current value
   * @param last last value or delta encoding
   */
  public long writeU64Delta(long value, long last) {
    final long delta = value - last;
    if (delta < 0) {
      throw new IllegalArgumentException("writeUInt64Delta with negative delta(" + delta + ")");
    }
    writeU64(delta);
    return value;
  }

  /**
   * Writes a signed 64bit integer varint encoded to the underlying Stream.
   *
   * @param value 64bit integer to encode.
   */
  public void writeS64(long value) {
    writeU64(encodeZigZag64(value));
  }

  public long writeS64Delta(long value, long last) {
    writeS64(value - last);
    return value;
  }

  public void writeByte(int value) {
    bos.write(value);
  }

  public void writeByteArray(byte[] value) throws IOException {
    bos.write(value);
  }

  public void writeByteArray(byte[] value, int offset, int length) {
    bos.write(value, offset, length);
  }

  private static int encodeZigZag32(final int n) {
    return n << 1 ^ n >> 31;
  }

  private static long encodeZigZag64(final long n) {
    return n << 1 ^ n >> 63;
  }

  public void reset() {
    bos.reset();
  }

  public int length() {
    return bos.length();
  }

  public byte[] array() {
    return bos.array();
  }
}
