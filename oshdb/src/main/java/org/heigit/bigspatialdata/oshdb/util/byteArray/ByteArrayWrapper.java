package org.heigit.bigspatialdata.oshdb.util.byteArray;

import java.io.IOException;
import java.util.Arrays;

public class ByteArrayWrapper {

  public static class InvalidProtocolBufferException extends IOException {
	private static final long serialVersionUID = 1L;

	public InvalidProtocolBufferException(final String description) {
      super(description);
    }
  }

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

  /** Read an {@code sint32} field value from the stream. */
  public int readSInt32() throws IOException {
    return decodeZigZag32(readRawVarint32());
  }
  
  public int readSInt32Delta(int last) throws IOException {
    return readSInt32() + last;
  }
  
  /** Read a {@code uint32} field value from the stream. */
  public int readUInt32() throws IOException {
    return readRawVarint32();
  }

  /** Read an {@code sint64} field value from the stream. */
  public long readSInt64() throws IOException {
    return decodeZigZag64(readRawVarint64());
  }
  public long readSInt64Delta(long last) throws IOException {
    return readSInt64() + last;
  }

  /** Read a {@code uint64} field value from the stream. */
  public long readUInt64() throws IOException {
    return readRawVarint64();
  }
  
  public long readUInt64Delta(long last) throws IOException {
    return readUInt64() + last;
  }

  /**
   * Read a raw Varint from the stream. If larger than 32 bits, discard the upper bits.
   */
  public int readRawVarint32() throws IOException {
    // See implementation notes for readRawVarint64
    fastpath: {
      int pos = bufferPos;

      if (bufferSize == pos) {
        break fastpath;
      }

      final byte[] buffer = this.buffer;
      int x;
      if ((x = buffer[pos++]) >= 0) {
        bufferPos = pos;
        return x;
      } else if (bufferSize - pos < 9) {
        break fastpath;
      } else if ((x ^= (buffer[pos++] << 7)) < 0L) {
        x ^= (~0L << 7);
      } else if ((x ^= (buffer[pos++] << 14)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14);
      } else if ((x ^= (buffer[pos++] << 21)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21);
      } else {
        int y = buffer[pos++];
        x ^= y << 28;
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
        if (y < 0 && buffer[pos++] < 0 && buffer[pos++] < 0 && buffer[pos++] < 0
            && buffer[pos++] < 0 && buffer[pos++] < 0) {
          break fastpath; // Will throw malformedVarint()
        }
      }
      bufferPos = pos;
      return x;
    }
    return (int) readRawVarint64SlowPath();
  }

  /** Read a raw Varint from the stream. */
  public long readRawVarint64() throws IOException {
    // Implementation notes:
    //
    // Optimized for one-byte values, expected to be common.
    // The particular code below was selected from various candidates
    // empirically, by winning VarintBenchmark.
    //
    // Sign extension of (signed) Java bytes is usually a nuisance, but
    // we exploit it here to more easily obtain the sign of bytes read.
    // Instead of cleaning up the sign extension bits by masking eagerly,
    // we delay until we find the final (positive) byte, when we clear all
    // accumulated bits with one xor. We depend on javac to constant fold.
    fastpath: {
      int pos = bufferPos;

      if (bufferSize == pos) {
        break fastpath;
      }

      final byte[] buffer = this.buffer;
      long x;
      int y;
      if ((y = buffer[pos++]) >= 0) {
        bufferPos = pos;
        return y;
      } else if (bufferSize - pos < 9) {
        break fastpath;
      } else if ((x = y ^ (buffer[pos++] << 7)) < 0L) {
        x ^= (~0L << 7);
      } else if ((x ^= (buffer[pos++] << 14)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14);
      } else if ((x ^= (buffer[pos++] << 21)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21);
      } else if ((x ^= ((long) buffer[pos++] << 28)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
      } else if ((x ^= ((long) buffer[pos++] << 35)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
      } else if ((x ^= ((long) buffer[pos++] << 42)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
      } else if ((x ^= ((long) buffer[pos++] << 49)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)
            ^ (~0L << 49);
      } else {
        x ^= ((long) buffer[pos++] << 56);
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)
            ^ (~0L << 49) ^ (~0L << 56);
        if (x < 0L) {
          if (buffer[pos++] < 0L) {
            break fastpath; // Will throw malformedVarint()
          }
        }
      }
      bufferPos = pos;
      return x;
    }
    return readRawVarint64SlowPath();
  }

  /** Variant of readRawVarint64 for when uncomfortably close to the limit. */
  /* Visible for testing */
  long readRawVarint64SlowPath() throws IOException {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      final byte b = readRawByte();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw malformedVarint();
  }

  /**
   * Read one byte from the input.
   *
   * @throws InvalidProtocolBufferException The end of the stream or the current limit was reached.
   */
  public byte readRawByte() throws IOException {
    if (bufferPos == bufferSize) {
      throw truncatedMessage();
    }
    return buffer[bufferPos++];
  }

  public byte[] readByteArray(int size) throws IOException {
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
  public static int decodeZigZag32(final int n) {
    return (n >>> 1) ^ -(n & 1);
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
  public static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  static InvalidProtocolBufferException malformedVarint() {
    return new InvalidProtocolBufferException("CodedInputStream encountered a malformed varint.");
  }

  static InvalidProtocolBufferException truncatedMessage() {
    return new InvalidProtocolBufferException(
        "While parsing a protocol message, the input ended unexpectedly "
            + "in the middle of a field.  This could mean either than the "
            + "input has been truncated or that an embedded message "
            + "misreported its own length.");
  }

  

  
}
