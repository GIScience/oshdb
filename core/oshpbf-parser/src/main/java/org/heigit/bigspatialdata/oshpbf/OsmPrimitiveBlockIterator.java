package org.heigit.bigspatialdata.oshpbf;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Fileformat;
import crosby.binary.Osmformat;
import crosby.binary.Fileformat.BlobHeader;
import crosby.binary.Osmformat.HeaderBlock;
import crosby.binary.Osmformat.PrimitiveBlock;

public class OsmPrimitiveBlockIterator implements Iterator<Osmformat.PrimitiveBlock>, Closeable{

	private final long start;
	private final long end;
	private long pos = 0;

	private final DataInputStream input;

	private HeaderInfo currentHeaderInfo;
	private PrimitiveBlock currentPrimitiveBlock;
	
	private long blockPos = -1;
	private long nextBlockPos = -1;
	

	public OsmPrimitiveBlockIterator(String filename) throws FileNotFoundException, IOException {
		this(new File(filename));
	}

	public OsmPrimitiveBlockIterator(File file) throws FileNotFoundException, IOException {
		this(new FileInputStream(file),file.length());
	}

	public OsmPrimitiveBlockIterator(InputStream is) throws IOException{
		this(is,-1);
	}
	
	
	public OsmPrimitiveBlockIterator(InputStream is, final long end) throws IOException {
		this.input = new DataInputStream(is);
		this.start = -1;
		this.end = end;
		seekForward();
	}

	private void seekForward() throws IOException {
		while (currentPrimitiveBlock == null) {
			nextBlockPos = readBlock();
			blockPos = nextBlockPos;
		}
	}

	private long readBlock() throws IOException {
	  long blockPos = -1;
		if (input.available() > 0) {
		    blockPos = pos;
			Fileformat.BlobHeader header = readBlobHeader();
			ByteString data = readData(header);
			try {
				if (header.getType().equals("OSMHeader")) {
					parseHeaderBlock(Osmformat.HeaderBlock.parseFrom(data));
				} else if (header.getType().equals("OSMData")) {
					currentPrimitiveBlock = Osmformat.PrimitiveBlock.parseFrom(data);
				}
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
				throw new Error("ParseError", e);
			}
		}
		
		return blockPos;
	}

	private void parseHeaderBlock(HeaderBlock headerblock) {
		HeaderInfo info = new HeaderInfo();

		if (headerblock.hasBbox()) {
			headerblock.getBbox();
		}

		for (int i = 0; i < headerblock.getRequiredFeaturesCount(); i++) {
			info.addRequiredFeatures(headerblock.getRequiredFeatures(i));
		}

		for (int i = 0; i < headerblock.getOptionalFeaturesCount(); i++) {
			info.addOptionalFeatures(headerblock.getOptionalFeatures(i));
		}

		if (headerblock.hasWritingprogram()) {
			info.setWrittingProgram(headerblock.getWritingprogram());
		}
		if (headerblock.hasSource()) {
			info.setSource(headerblock.getSource());
		}

		if (headerblock.hasOsmosisReplicationTimestamp()) {
			info.setReplicationTimestamp(headerblock.getOsmosisReplicationTimestamp());
		}

		if (headerblock.hasOsmosisReplicationSequenceNumber()) {
			info.setReplicationSequenceNumber(headerblock.getOsmosisReplicationSequenceNumber());
		}

		if (headerblock.hasOsmosisReplicationBaseUrl()) {
			info.setReplicationBaseUrl(headerblock.getOsmosisReplicationBaseUrl());
		}

		currentHeaderInfo = info;
	}

	private ByteString readData(BlobHeader header) throws IOException {
		byte[] buf = new byte[header.getDatasize()];
		input.readFully(buf);
		pos += buf.length;
		Fileformat.Blob blob = Fileformat.Blob.parseFrom(buf);
		ByteString data = ByteString.EMPTY;

		if (blob.hasRaw()) {
			data = blob.getRaw();
		} else if (blob.hasZlibData()) {
			byte buf2[] = new byte[blob.getRawSize()];
			Inflater decompresser = new Inflater();
			decompresser.setInput(blob.getZlibData().toByteArray());
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

	private BlobHeader readBlobHeader() throws IOException {
		int headersize = input.readInt();
		byte[] buf = new byte[headersize];
		input.readFully(buf);
		pos += 4 + headersize;
		return Fileformat.BlobHeader.parseFrom(buf);
	}

	public float getProgress() {
		if (end > -1)
			return ((float) pos - start) / ((float) end - start);
		else
			return -1.0f;
	}

	@Override
	public boolean hasNext() {
		return currentPrimitiveBlock != null;
	}

	@Override
	public PrimitiveBlock next() {
		PrimitiveBlock block = currentPrimitiveBlock;
		currentPrimitiveBlock = null;
		blockPos = nextBlockPos;
		try {
			nextBlockPos = readBlock();
		} catch (IOException e) {
			if (e instanceof EOFException) {
				;
			} else {
				// TODO LOG Error
				e.printStackTrace();
			}
		}
		return block;
	}

	public HeaderInfo getHeaderInfo() {
		return currentHeaderInfo;
	}

  public long getBlockPos() {
    return blockPos;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

}
