package org.heigit.ohsome.oshdb.util.bytearray;

import java.io.IOException;

public class ByteArrayOutputWrapper {

  protected final OSHDBByteArrayOutputStream bos;

  public ByteArrayOutputWrapper(int bufferSize) {
    bos = new OSHDBByteArrayOutputStream(bufferSize);
  }

  public ByteArrayOutputWrapper() {
    this(256);
  }

  public void writeU32(int value) {
    while ((value & ~0x7F) != 0) {
      bos.write(value & 0x7F | 0x80);
      value >>>= 7;
    }
    bos.write(value);
  }

  public void writeS32(int value) throws IOException {
    writeU32(encodeZigZag32(value));
  }

  public int writeS32Delta(int value, int last) throws IOException {
    writeS32(value - last);
    return value;
  }

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
  public void writeU64Delta(long value, long last) throws IOException {
    final long delta = value - last;
    if (delta < 0) {
      throw new IllegalArgumentException("writeUInt64Delta with negative delta(" + delta + ")");
    }
    writeU64(delta);
  }

  public void writeS64(long value) throws IOException {
    writeU64(encodeZigZag64(value));
  }

  public long writeS64Delta(long value, long last) throws IOException {
    writeS64(value - last);
    return value;
  }

  public void writeByte(int value) {
    bos.write(value);
  }

  public void writeByteArray(byte[] value) throws IOException {
    bos.write(value);
  }

  public void writeByteArray(final byte[] value, int offset, int length) {
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

  public OSHDBByteArrayOutputStream getByteArrayStream() {
    return bos;
  }
}
