package org.heigit.bigspatialdata.oshpbf;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Iterator;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Fileformat;
import crosby.binary.Fileformat.BlobHeader;
import crosby.binary.Osmformat;
import crosby.binary.Osmformat.HeaderBlock;
import crosby.binary.Osmformat.PrimitiveBlock;

public class OsmPrimitiveBlockIterator implements Iterator<Osmformat.PrimitiveBlock>, Closeable {
	/**
	 * 
	 * The PBF BlobHeader is a repeating sequence of: - int4: length of the
	 * BlobHeader message in network byte order - serialized BlobHeader
	 * message + required string type = 1; + optional bytes indexdata = 2; +
	 * required int32 datasize = 3;
	 * 
	 * http://wiki.openstreetmap.org/wiki/PBF_Format#File_format
	 * 
	 */
	public static final int BlobHeaderLength = 4;

	/**
	 * 
	 * Tag((field_number << 3) | wire_type => 10)+sizeOfString
	 * +"OSMData".getBytes();
	 * 
	 */
	public static final byte[] SIGNATURE_OSMDATA = { 10, 7, 79, 83, 77, 68, 97, 116, 97 };
	private static final byte[] SIGNATURE_OSMHEADER = { 10, 9, 79, 83, 77, 72, 101, 97, 100, 101, 114 };
	
	private final long start;
	private final long end;
	private long pos = 0;
	private boolean EOF = false;

	private final DataInputStream input;

	private HeaderInfo currentHeaderInfo;
	private PrimitiveBlock nextPrimitiveBlock;

	private long blockPos = 0;
	private long nextBlockPos = 0;

	public OsmPrimitiveBlockIterator(String filename) throws FileNotFoundException, IOException {
		this(new File(filename));
	}

	public OsmPrimitiveBlockIterator(File file) throws FileNotFoundException, IOException {
		this(new FileInputStream(file), file.length());
	}

	public OsmPrimitiveBlockIterator(InputStream is) throws IOException {
		this(is, -1);
	}

	public OsmPrimitiveBlockIterator(InputStream is, final long end) throws IOException {
		this.input = new DataInputStream(findSignature(is));
		this.start = -1;
		this.end = end;
		seekForward();
	}
	
	private InputStream findSignature(InputStream is) throws IOException {
		byte[] pushBackBytes = new byte[BlobHeaderLength
				+ Math.max(SIGNATURE_OSMDATA.length, SIGNATURE_OSMHEADER.length)];
		PushbackInputStream pushBackStream = new PushbackInputStream(is, pushBackBytes.length);
		for (int i = 0; i < 4; i++) {
			pushBackBytes[i] = (byte) pushBackStream.read();
			pos++;
		}
		
		int nextByte = pushBackStream.read();
		pos++;
		int val = 0;
		while (nextByte != -1) {
			if ((val < SIGNATURE_OSMDATA.length && SIGNATURE_OSMDATA[val] == nextByte)
					|| SIGNATURE_OSMHEADER[val] == nextByte) {
				pushBackBytes[BlobHeaderLength + val] = (byte) nextByte;
				if ((val < SIGNATURE_OSMDATA.length && SIGNATURE_OSMDATA[val] == nextByte
						&& val == SIGNATURE_OSMDATA.length - 1)
						|| (SIGNATURE_OSMHEADER[val] == nextByte && val == SIGNATURE_OSMHEADER.length - 1)) {
					// Full OSMData SIGNATURE is found.
					pushBackStream.unread(pushBackBytes, 0, BlobHeaderLength + val + 1);
					pos -= BlobHeaderLength + val + 1;
					return pushBackStream;
				}
				val++;
			} else if (val != 0) {
				val = 0;
				if (SIGNATURE_OSMDATA[val] == nextByte || SIGNATURE_OSMHEADER[val] == nextByte) {
					pushBackBytes[BlobHeaderLength + val] = (byte) nextByte;
					val++;
				} else {
					for (int i = 0; i < 3; i++) {
						pushBackBytes[i] = pushBackBytes[i + 1];
					}
					pushBackBytes[3] = (byte) nextByte;
				}

			} else {
				for (int i = 0; i < 3; i++) {
					pushBackBytes[i] = pushBackBytes[i + 1];
				}
				pushBackBytes[3] = (byte) nextByte;
			}

			nextByte = pushBackStream.read();
			pos++;
		}

		return is;
	}

	private void seekForward() throws IOException {
		while (nextPrimitiveBlock == null && (end == -1 || pos < end) && !EOF) {
			readNextBlock();
		}
	}

	private void readNextBlock() {
		long blockPos = pos;
		PrimitiveBlock block = null;

		if (end == -1 || pos < end) {
			try {
				
				Fileformat.BlobHeader header = readBlobHeader();
				ByteString data = readData(header);
				try {
					if (header.getType().equals("OSMHeader")) {
						parseHeaderBlock(Osmformat.HeaderBlock.parseFrom(data));
					} else if (header.getType().equals("OSMData")) {
						block = Osmformat.PrimitiveBlock.parseFrom(data);
					}
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
					throw new Error("ParseError", e);
				}
			} catch (IOException e) {
				EOF = true;
				block = null;
				if (!(e instanceof EOFException)) {
					e.printStackTrace();
				}
			}
		}
		nextBlockPos = blockPos;
		nextPrimitiveBlock = block;
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
		return nextPrimitiveBlock != null;
	}

	@Override
	public PrimitiveBlock next() {
		PrimitiveBlock block = nextPrimitiveBlock;
		nextPrimitiveBlock = null;
		blockPos = nextBlockPos;
		readNextBlock();
		return block;
	}

	public HeaderInfo getHeaderInfo() {
		return currentHeaderInfo;
	}

	public long getBlockPos() {
		return blockPos;
	}
	
	public long getNextBlockPos(){
		return nextBlockPos;
	}

	@Override
	public void close() throws IOException {
		input.close();
	}

}
