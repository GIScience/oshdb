package org.heigit.ohsome.oshdb.io;

import java.nio.ByteBuffer;
import org.heigit.ohsome.oshdb.util.BitUtil;
import org.heigit.ohsome.oshdb.util.BytesUtil;

public class OSHDBInput {

  public static OSHDBInput wrap(ByteBuffer buffer) {
    return wrap(buffer.array(), buffer.position(), buffer.remaining());
  }

  public static OSHDBInput wrap(byte[] bytes) {
    return wrap(bytes, 0, bytes.length);
  }

  public static OSHDBInput wrap(byte[] bytes, int offset, int length) {
    return new OSHDBInput(bytes, offset, length);
  }

  private byte[] bytes;
  private int pos;
  private int limit;

  private OSHDBInput(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.pos = offset;
    this.limit = offset + length;
  }

  /**
   * Returns the number of elements between the current position and the limit.
   *
   * @return The number of elements remaining in this buffer
   */
  public int remaining() {
    return limit - pos;
  }

  /**
   * Tells whether there are any elements between the current position and the limit.
   *
   * @return {@code true} if, and only if, there is at least one element remaining in this buffer
   */
  public final boolean hasRemaining() {
    return pos < limit;
  }

  /**
   * Creates a new OSHDBInput whose content is a shared subsequence of this OSHDBInput's content.
   *
   * <p>
   * The content of the new OSHDBInput will start at this OSHDBInput's current position. The two
   * OSHDBInput' position, limit, will be independent.
   * </p>
   *
   * @return The new OSHDBInput
   */
  public OSHDBInput slice() {
    return new OSHDBInput(bytes, pos, limit - pos);
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(bytes, pos, limit);
  }

  public int readByte() {
    return bytes[pos++];
  }

  public boolean readBool() {
    return readByte() != 0;
  }

  public ByteBuffer readBytes(int len) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes, pos, len);
    pos += len;
    return buf;
  }

  public ByteBuffer readBytes() {
    final int len = readUInt32();
    return readBytes(len);
  }

  public String readUTF8() {
    var buffer = readBytes();
    return BytesUtil.string(buffer);
  }

  public int readUInt32() {
    int i;
    if ((i = readByte()) >= 0) {
      return i;
    } else if ((i ^= readByte() << 7) < 0) {
      return i ^ ~0 << 7;
    } else if ((i ^= readByte() << 14) >= 0) {
      return i ^ ~0 << 7 ^ ~0 << 14;
    } else if ((i ^= readByte() << 21) < 0) {
      return i ^ ~0 << 7 ^ ~0 << 14 ^ ~0 << 21;
    } else {
      i ^= readByte() << 28;
      return i ^ ~0 << 7 ^ ~0 << 14 ^ ~0 << 21 ^ ~0 << 28;
    }
  }

  public long readUInt64() {
    long l;
    int i;
    if ((i = readByte()) >= 0) {
      return i;
    } else if ((i ^= readByte() << 7) < 0) {
      return i ^ ~0 << 7;
    } else if ((i ^= readByte() << 14) >= 0) {
      return i ^ ~0 << 7 ^ ~0 << 14;
    } else if ((i ^= readByte() << 21) < 0) {
      return i ^ ~0 << 7 ^ ~0 << 14 ^ ~0 << 21;
    } else if ((l = i ^ (long) readByte() << 28) >= 0L) {
      return l ^ ~0L << 7 ^ ~0L << 14 ^ ~0L << 21 ^ ~0L << 28;
    } else if ((l ^= (long) readByte() << 35) < 0L) {
      return l ^ ~0L << 7 ^ ~0L << 14 ^ ~0L << 21 ^ ~0L << 28 ^ ~0L << 35;
    } else if ((l ^= (long) readByte() << 42) >= 0L) {
      return l ^ ~0L << 7 ^ ~0L << 14 ^ ~0L << 21 ^ ~0L << 28 ^ ~0L << 35 ^ ~0L << 42;
    } else if ((l ^= (long) readByte() << 49) < 0L) {
      return l ^ ~0L << 7 ^ ~0L << 14 ^ ~0L << 21 ^ ~0L << 28 ^ ~0L << 35 ^ ~0L << 42 ^ ~0L << 49;
    } else {
      l ^= (long) readByte() << 56;
      return l ^ ~0L << 7 ^ ~0L << 14 ^ ~0L << 21 ^ ~0L << 28 ^ ~0L << 35 ^ ~0L << 42 ^ ~0L << 49
          ^ ~0L << 56;
    }
  }

  public int readSInt32() {
    return BitUtil.zigZagDecode(readUInt32());
  }

  public long readSInt64() {
    return BitUtil.zigZagDecode(readUInt64());
  }

  public int pos() {
    return pos;
  }

  public void skip(int length) {
    pos += length;
  }
}
