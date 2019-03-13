package org.heigit.bigspatialdata.oshpbf.parser.pbf;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Fileformat;
import crosby.binary.Osmformat;

/**
 * A full OSM Pbf Blob. Contains both, <a href=
 * "http://wiki.openstreetmap.org/wiki/PBF_Format#File_format">BlobHeader(header)
 * and Blob(content)</a>. In addition this class hold the position within the
 * 
 * 
 * inputstream for this block.
 * 
 * @author rtroilo
 *
 */
public class PbfBlob {

	public final long pos;
	public final Fileformat.BlobHeader header;
	public final Fileformat.Blob content;
	
	public final boolean isFirstBlob;
	public final boolean overSoftLimit;
	
	
	private Osmformat.PrimitiveBlock block = null;
	
	
	public PbfBlob(final long pos, final Fileformat.BlobHeader header, final Fileformat.Blob blob,boolean isFirstBlob,boolean overSoftLimit) {
		this.pos = pos;
		this.header = header;
		this.content = blob;
		this.isFirstBlob = isFirstBlob;
		this.overSoftLimit = overSoftLimit;
	}
	
	public long getBlobPos(){
	  return pos;
	}

	public boolean isHeader() {
		return header.getType().equals("OSMHeader");
	}

	public boolean isData() {
		return header.getType().equals("OSMData");
	}

	public Osmformat.HeaderBlock getHeaderBlock() throws InvalidProtocolBufferException {
		if (isHeader()) {
			final ByteString data = getData();
			return Osmformat.HeaderBlock.parseFrom(data);
		}
		return null;
	}

	public Osmformat.PrimitiveBlock getPrimitivBlock() throws InvalidProtocolBufferException {
		if (isData()) {
		  if(block != null)
		    return block;
		  
			final ByteString data = getData();
			//block = Osmformat.PrimitiveBlock.parseFrom(data); 
			return Osmformat.PrimitiveBlock.parseFrom(data);
		}
		return null;
	}

	public ByteString getData() {
		ByteString data = ByteString.EMPTY;

		if (content.hasRaw()) {
			data = content.getRaw();
		} else if (content.hasZlibData()) {
			byte buf2[] = new byte[content.getRawSize()];
			Inflater decompresser = new Inflater();
			decompresser.setInput(content.getZlibData().toByteArray());
			try {
				decompresser.inflate(buf2);
			} catch (DataFormatException e) {
				e.printStackTrace();
				throw new Error(e);
			}
			assert (decompresser.finished());
			decompresser.end();
			data = ByteString.copyFrom(buf2);
		}
		return data;
	}
	
	@Override
	public String toString() {
	  return String.format("PbfBlob:%d  isData:%s", pos, isData());
	}

}
