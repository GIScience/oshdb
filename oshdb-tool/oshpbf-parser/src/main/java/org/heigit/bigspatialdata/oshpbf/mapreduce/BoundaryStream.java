package org.heigit.bigspatialdata.oshpbf.mapreduce;

import java.io.EOFException;
import java.io.IOException;


public class BoundaryStream extends RandomAccessInputStream {

  /** Base input stream. */
  private final RandomAccessInputStream is;

  /** Start position. */
  private final long start;

  /** End position. */
  private final long end;

  /** Current position within the stream. */
  private long pos;


  public BoundaryStream(RandomAccessInputStream is, long start, long end) throws IOException {
    if (is == null)
      throw new IllegalArgumentException("Input stream cannot be null.");

    if (start < 0)
      throw new IllegalArgumentException("Start position cannot be negative.");

    if (start >= is.length())
      throw new IllegalArgumentException("Start position cannot be greater that file length.");

    if (end < 0)
      throw new IllegalArgumentException("End position cannot be negative.");

    if (end > is.length())
      throw new IllegalArgumentException("End position cannot be greater than file length.");

    this.is = is;
    this.start = start;
    this.end = end;

    is.seek(start);
  }

  public BoundaryStream(RandomAccessInputStream is, Boundary bounds) throws IOException {
    this(is, bounds.getStart(), bounds.getEnd());
  }

  /** {@inheritDoc} 
   * @throws IOException */
  @Override public long length() throws IOException {
      return is.length();
  }

  
  /** {@inheritDoc} */
  @Override
  public int read() throws IOException {
    if (pos < end) {
      int res = is.read();

      if (res != -1)
        pos++;

      return res;
    } else
      return -1;
  }

  /** {@inheritDoc} */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (pos < end) {
      len = (int) Math.min(len, end - pos);

      int res = is.read(b, off, len);

      if (res != -1)
        pos += res;

      return res;
    } else
      return -1;
  }

  /** {@inheritDoc} */
  @Override
  public int read(long pos, byte[] buf, int off, int len) throws IOException {
    seek(pos);

    return read(buf, off, len);
  }

  /** {@inheritDoc} */
  @Override
  public void readFully(long pos, byte[] buf) throws IOException {
    readFully(pos, buf, 0, buf.length);
  }

  /** {@inheritDoc} */
  @Override
  public void readFully(long pos, byte[] buf, int off, int len) throws IOException {
    seek(pos);

    for (int readBytes = 0; readBytes < len;) {
      int read = read(buf, off + readBytes, len - readBytes);

      if (read == -1)
        throw new EOFException("Failed to read stream fully (stream ends unexpectedly) [pos=" + pos
            + ", buf.length=" + buf.length + ", off=" + off + ", len=" + len + ']');

      readBytes += read;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0)
      throw new IOException("Seek position cannot be negative: " + pos);

    is.seek(start + pos);

    this.pos = pos;
  }

  /** {@inheritDoc} */
  @Override
  public long position() {
    return pos;
  }

  /**
   * Since range input stream represents a part of larger file stream, there is an offset at which
   * this range input stream starts in original input stream. This method returns start offset of
   * this input stream relative to original input stream.
   *
   * @return Start offset in original input stream.
   */
  public long startOffset() {
    return start;
  }

  /** {@inheritDoc} */
  @Override
  public int available() {
    long l = end - (start + pos);

    if (l < 0)
      return 0;

    if (l > Integer.MAX_VALUE)
      return Integer.MAX_VALUE;

    return (int) l;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    is.close();
  }

}
