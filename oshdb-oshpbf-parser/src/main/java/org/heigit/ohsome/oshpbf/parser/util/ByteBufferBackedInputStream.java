package org.heigit.ohsome.oshpbf.parser.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {
  protected final ByteBuffer buf;

  public ByteBufferBackedInputStream(ByteBuffer buf) {
    this.buf = buf;
  }

  @Override
  public int available() {
    return buf.remaining();
  }

  @Override
  public int read() throws IOException {
    return buf.hasRemaining() ? buf.get() & 0xFF : -1;
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }
}
