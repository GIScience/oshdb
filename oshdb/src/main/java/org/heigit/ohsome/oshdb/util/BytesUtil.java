package org.heigit.ohsome.oshdb.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

public class BytesUtil {
  private BytesUtil() {}

  public static Charset UTF8 = Charset.forName("UTF-8");
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(EMPTY_BYTES);

  public static byte[] readFully(InputStream in, int length) throws IOException {
    var bytes = new byte[length];
    readFully(in, bytes, 0, bytes.length);
    return bytes;
  }

  public static void readFully(InputStream in, byte[] buffer) throws IOException {
    readFully(in, buffer, 0, buffer.length);
  }

  public static void readFully(InputStream in, byte[] buffer, int off, int len) throws IOException {
    if (len < 0 || off < 0 || len + off > buffer.length) {
      throw new IndexOutOfBoundsException();
    }
    var n = 0;
    while (n < len) {
      var count = in.read(buffer, off + n, len - n);
      if (count < 0) {
        throw new EOFException();
      }
      n += count;
    }
  }

  public static String string(ByteBuffer buffer) {
    return new String(buffer.array(), buffer.position(), buffer.remaining(), UTF8);
  }

  public static ByteBuffer copy(ByteBuffer buffer) {
    byte[] bytes = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
    return ByteBuffer.wrap(bytes);
  }

  public static String hRBC(long bytes) {
    var unit = 1024;
    if (bytes < unit) {
      return bytes + " B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    final String pre = "" + "kMGTPE".charAt(exp - 1);
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

  public static int readInt(InputStream input) throws EOFException, IOException {
    int result = 0;
    for (int i = 0; i < 4; i++) {
      var b = input.read();
      if (b < 0) {
        throw new EOFException();
      }
      result = (result << 8) + b;
    }
    return result;
  }
}
