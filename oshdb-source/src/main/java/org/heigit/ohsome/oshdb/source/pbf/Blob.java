package org.heigit.ohsome.oshdb.source.pbf;

import com.google.protobuf.InvalidProtocolBufferException;
import crosby.binary.Fileformat;
import crosby.binary.Osmformat;
import crosby.binary.file.FileFormatException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import reactor.core.publisher.Mono;

public class Blob {

  private static final int MAX_HEADER_SIZE = 64 * 1024;

  public static Blob read(InputStream input) throws IOException {
    DataInputStream dataInput = new DataInputStream(input);
    var headerSize = dataInput.readInt();
    if (headerSize > MAX_HEADER_SIZE) {
      throw new FileFormatException(
          "Unexpectedly long header " + MAX_HEADER_SIZE + " bytes. Possibly corrupt file.");
    }

    var buf = new byte[headerSize];
    dataInput.readFully(buf);
    var header = Fileformat.BlobHeader.parseFrom(buf);

    var offset = position(input);

    var data = new byte[header.getDatasize()];
    dataInput.readFully(data);

    return new Blob(header.getType(), offset, data);
  }

  private static long position(InputStream input) throws IOException {
    if (input instanceof FileInputStream in) {
      return in.getChannel().position();
    }
    return -1;
  }

  private final String type;
  private final long offset;
  private final byte[] data;

  private Blob(String type, long offset, byte[] data) {
    this.type = type;
    this.offset = offset;
    this.data = data;
  }

  @Override
  public String toString() {
    return "Blob{" +
        "type='" + type + '\'' +
        ", offset=" + offset +
        ", data=" + data.length +
        "bytes";
  }

  public long offset() {
    return offset;
  }

  public byte[] data() {
    return data;
  }

  public boolean isHeader() {
    return "OSMHeader".equals(type);
  }

  public Osmformat.HeaderBlock header() throws FileFormatException {
    if (!isHeader()) {
      throw new NoSuchElementException();
    }

    try {
      return Osmformat.HeaderBlock.parseFrom(decompress());
    } catch (InvalidProtocolBufferException e) {
      throw new FileFormatException(e);
    }
  }

  public boolean isData() {
    return "OSMData".equals(type);
  }

  public Mono<Block> block() {
    if (!isData()) {
      return Mono.error(new NoSuchElementException());
    }
    return Mono.fromCallable(() -> Block.parse(this, decompress()));
  }

  private byte[] decompress() throws FileFormatException {
    var blob = parseBlob();
    if (blob.hasRaw()) {
      return blob.getRaw().toByteArray();
    }
    if (blob.hasZlibData()) {
      return decompress(blob);
    }
    throw new UnsupportedOperationException();
  }

  private static byte[] decompress(Fileformat.Blob blob) throws FileFormatException {
    var buffer = new byte[blob.getRawSize()];
    Inflater inflater = new Inflater();
    try {
      inflater.setInput(blob.getZlibData().toByteArray());
      inflater.inflate(buffer);
      assert (inflater.finished());
    } catch (DataFormatException e) {
      throw new FileFormatException(e);
    } finally {
      inflater.end();
    }
    return buffer;
  }

  private Fileformat.Blob parseBlob() throws FileFormatException {
    try {
      return Fileformat.Blob.parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      throw new FileFormatException(e);
    }
  }
}
