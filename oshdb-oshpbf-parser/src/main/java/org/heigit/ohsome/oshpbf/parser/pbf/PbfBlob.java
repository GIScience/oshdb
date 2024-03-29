package org.heigit.ohsome.oshpbf.parser.pbf;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import crosby.binary.Fileformat;
import crosby.binary.Osmformat;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * A full OSM Pbf Blob. Contains both, <a href=
 * "http://wiki.openstreetmap.org/wiki/PBF_Format#File_format">BlobHeader(header)
 * and Blob(content)</a>. In addition this class hold the position within the inputstream for
 * this block.
 */
public class PbfBlob {

  public final long pos;
  public final Fileformat.BlobHeader header;
  public final Fileformat.Blob content;

  public final boolean isFirstBlob;
  public final boolean overSoftLimit;

  private Osmformat.PrimitiveBlock block = null;

  /**
   * Create a {@code PbfBlob} from {@code Fileformat.Blob}.
   */
  public PbfBlob(final long pos, final Fileformat.BlobHeader header, final Fileformat.Blob blob,
      boolean isFirstBlob, boolean overSoftLimit) {
    this.pos = pos;
    this.header = header;
    this.content = blob;
    this.isFirstBlob = isFirstBlob;
    this.overSoftLimit = overSoftLimit;
  }

  public long getBlobPos() {
    return pos;
  }

  public boolean isHeader() {
    return header.getType().equals("OSMHeader");
  }

  public boolean isData() {
    return header.getType().equals("OSMData");
  }

  /**
   * Returns {@code Osmformat.HeaderBlock} if {@code PbfBlob} is a HeaderBlob else {@code null}.
   */
  public Osmformat.HeaderBlock getHeaderBlock() throws InvalidProtocolBufferException {
    if (isHeader()) {
      final ByteString data = getData();
      return Osmformat.HeaderBlock.parseFrom(data);
    }
    return null;
  }

  /**
   * Returns {@code Osmformat.PrimitiveBlock} if {@code PbfBlob} is a DataBlob else {@code null}.
   */
  public Osmformat.PrimitiveBlock getPrimitivBlock() throws InvalidProtocolBufferException {
    if (isData()) {
      if (block != null) {
        return block;
      }

      final ByteString data = getData();
      //block = Osmformat.PrimitiveBlock.parseFrom(data);
      return Osmformat.PrimitiveBlock.parseFrom(data);
    }
    return null;
  }

  /**
   * Decompress if necessary and returns the decompressed Data {@code ByteString}.
   */
  public ByteString getData() {
    ByteString data = ByteString.EMPTY;

    if (content.hasRaw()) {
      data = content.getRaw();
    } else if (content.hasZlibData()) {
      byte[] buf2 = new byte[content.getRawSize()];
      Inflater decompresser = new Inflater();
      try {
        decompresser.setInput(content.getZlibData().toByteArray());
        decompresser.inflate(buf2);
        assert decompresser.finished();
      } catch (DataFormatException e) {
        throw new Error(e);
      } finally {
        decompresser.end();
      }
      data = ByteString.copyFrom(buf2);
    }
    return data;
  }

  @Override
  public String toString() {
    return String.format("PbfBlob:%d  isData:%s", pos, isData());
  }

}
