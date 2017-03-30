package org.heigit.bigspatialdata.oshpbf.mapreduce;

import java.io.IOException;
import java.io.InputStream;

public abstract class RandomAccessInputStream extends InputStream {


  /**
   * Gets file length during file open.
   *
   * @return File length.
   * @throws IOException 
   */
  public abstract long length() throws IOException;

  /**
   * Get the current position in the input stream.
   *
   * @return The current position in the input stream.
   * @throws IOException In case of IO exception.
   */
  public abstract long position() throws IOException;


  /**
   * Seek to the specified position.
   *
   * @param pos Position to seek to.
   * @throws IOException In case of IO exception.
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * Read bytes from the given position in the stream to the given buffer. Continues to read until
   * passed buffer becomes filled.
   *
   * @param pos Position in the input stream to seek.
   * @param buf Buffer into which data is read.
   * @throws IOException In case of IO exception.
   */
  public abstract void readFully(long pos, byte[] buf) throws IOException;

  /**
   *
   * @param pos Position in the input stream to seek.
   * @param buf Buffer into which data is read.
   * @param off Offset in the buffer from which stream data should be written.
   * @param len The number of bytes to read.
   * @throws IOException In case of IO exception.
   */
  public abstract void readFully(long pos, byte[] buf, int off, int len) throws IOException;

  /**
   *
   * @param pos Position in the input stream to seek.
   * @param buf Buffer into which data is read.
   * @param off Offset in the buffer from which stream data should be written.
   * @param len The number of bytes to read.
   * @return Total number of bytes read into the buffer, or -1 if there is no more data (EOF).
   * @throws IOException In case of IO exception.
   */
  public abstract int read(long pos, byte[] buf, int off, int len) throws IOException;

}
