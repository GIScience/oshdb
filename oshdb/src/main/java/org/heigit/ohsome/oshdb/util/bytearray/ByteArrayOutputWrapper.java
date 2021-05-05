package org.heigit.ohsome.oshdb.util.bytearray;

import com.google.protobuf.CodedOutputStream;
import java.io.IOException;

public class ByteArrayOutputWrapper {

  final OSHDBByteArrayOutputStream bos;
  final CodedOutputStream cos;

  public ByteArrayOutputWrapper(int bufferSize) {
    bos = new OSHDBByteArrayOutputStream(bufferSize);
    cos = CodedOutputStream.newInstance(bos, bufferSize);
  }

  public ByteArrayOutputWrapper() {
    this(256);
  }

  public void writeU32(int value) throws IOException {
    cos.writeUInt32NoTag(value);
  }

  public void writeS32(int value) throws IOException {
    cos.writeSInt32NoTag(value);
  }

  public int writeS32Delta(int value, int last) throws IOException {
    writeS32(value - last);
    return value;
  }

  public void writeU64(long value) throws IOException {
    cos.writeUInt64NoTag(value);
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
    cos.writeSInt64NoTag(value);
  }

  public long writeS64Delta(long value, long last) throws IOException {
    writeS64(value - last);
    return value;
  }

  public void writeByte(byte value) throws IOException {
    cos.writeRawByte(value);
  }

  public void writeByteArray(byte[] value) throws IOException {
    cos.writeRawBytes(value);
  }

  public void writeByteArray(final byte[] value, int offset, int length) throws IOException {
    cos.writeRawBytes(value, offset, length);
  }

  public void reset() {
    bos.reset();
  }

  public int length() throws IOException {
    cos.flush();
    return bos.length();
  }

  public byte[] array() throws IOException {
    cos.flush();
    return bos.array();
  }

  public OSHDBByteArrayOutputStream getByteArrayStream() throws IOException {
    cos.flush();
    return bos;
  }
}
