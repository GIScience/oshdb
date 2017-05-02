package org.heigit.bigspatialdata.oshdb_tool;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshpbf.OsmPbfBlocks;
import org.heigit.bigspatialdata.oshpbf.OsmPbfIterator;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import org.heigit.bigspatialdata.oshpbf.mapreduce.Boundary;
import org.heigit.bigspatialdata.oshpbf.mapreduce.BoundaryStream;
import org.heigit.bigspatialdata.oshpbf.mapreduce.RandomAccessInputStream;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity;

import com.google.common.io.CountingInputStream;

import crosby.binary.Osmformat.PrimitiveBlock;

public class OSHDbTool {
	
	private static final int MB = 1024*1024;
	private static final long CHUNKSIZE = 64 * MB;

	public static class SplitInputStream {
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

		private static InputStream findSignature(InputStream is) throws IOException {
			byte[] pushBackBytes = new byte[BlobHeaderLength
					+ Math.max(SIGNATURE_OSMDATA.length, SIGNATURE_OSMHEADER.length)];
			PushbackInputStream pushBackStream = new PushbackInputStream(is, pushBackBytes.length);
			int pos = 0;
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

	}

	public static void main(String[] args) throws FileNotFoundException, IOException {

		String baseOSHDir = "E:/heigit/data";
		String oshBW = "heidelberg-ccbysa.osh.pbf";

		String pbf = Paths.get(baseOSHDir, oshBW).toString();
		System.out.println(pbf);

		final File file = new File(pbf);

		long fl = file.length();
		// FileInputStream fis = new FileInputStream(file);

		// fis.skip(240000000);

		// DataInputStream input;
		// input = new DataInputStream(SplitInputStream.findSignature(fis));

		final long chunk = 1024 * 1024; // MB;

		List<Long> chunks = new ArrayList<>();

		long chunkStart = 0;
		while (chunkStart < fl) {
			chunks.add(chunkStart);
			chunkStart += chunk;
		}
/*
		{
			try (FileInputStream fis = new FileInputStream(file)) {
				DataInputStream input = new DataInputStream(SplitInputStream.findSignature(fis));
				OsmPbfBlocks blocks = new OsmPbfBlocks(input, -1);
				long c1 = 0;
				OsmPrimitiveBlockIterator itr = (OsmPrimitiveBlockIterator) blocks.iterator();
				while (itr.hasNext()) {
					itr.next();
					c1++;
					System.out.println(itr.getBlockPos());
				}
				System.out.println(c1);
			}
		}
*/

		long cc = 0;
		for (int i = 0; i < chunks.size(); i++) {
			long start = chunks.get(i);
			long end = start + chunk;
			FileInputStream fis = new FileInputStream(file);
			fis.skip(start);
			CountingInputStream cis = new CountingInputStream(fis);	
			
			DataInputStream input = new DataInputStream(SplitInputStream.findSignature(cis));
			
			OsmPbfBlocks blocks = new OsmPbfBlocks(input, end - (start+cis.getCount()-13));

			long c1 = 0;
			Iterator itr = blocks.iterator();
			while (itr.hasNext()) {
				itr.next();
				c1++;
			}
			System.out.printf("%d - %d -> %d\n", start, start + chunk, c1);

			cc += c1;
		}

		System.out.println(cc);
	

		
	/*	
		byte[] buf = {
				1,2,2,2,2,2,
				0,0,0,0, 10, 7, 79, 83, 77, 68, 97, 116, 97 
				,6,6,6,6,6,6,6,6
		};
		
		SplitInputStream.findSignature(new ByteArrayInputStream(buf));
		
	*/	
		
		
/*		
		try (FileInputStream fis = new FileInputStream(file)) {
			long start = 2077146+1;
			long end = -1;
			fis.skip(start);
			DataInputStream input = new DataInputStream(SplitInputStream.findSignature(fis));
			OsmPbfBlocks blocks = new OsmPbfBlocks(input,-1);
			OsmPrimitiveBlockIterator itr = (OsmPrimitiveBlockIterator) blocks.iterator();
			OsmPbfIterator pbfItr = new OsmPbfIterator(itr);
			while (pbfItr.hasNext()) {
				OSMPbfEntity e = pbfItr.next();
				System.out.println(e);
				break;
			}
			
		}
*/		
		
		/*
		
		
		{
			try (FileInputStream fis = new FileInputStream(file)) {
				long start = 2091248;
				long end = -1;
				fis.skip(start+29480-2);
				DataInputStream input = new DataInputStream(SplitInputStream.findSignature(fis));
				OsmPbfBlocks blocks = new OsmPbfBlocks(input, end -start);
				long c1 = 0;
				OsmPrimitiveBlockIterator itr = (OsmPrimitiveBlockIterator) blocks.iterator();
				OsmPbfIterator pbfItr = new OsmPbfIterator(itr);
				long blockPos = -1;
				
				while (pbfItr.hasNext()) {
					OSMPbfEntity e = pbfItr.next();
					if(e.getId() == 374231942){
						System.out.println("heire "+ start+itr.getBlockPos());
						break;
					}
					
					if(itr.getBlockPos() != blockPos){
						blockPos = itr.getBlockPos();
						c1++;
						//System.out.printf("%d  %d\n",start+itr.getBlockPos(),e.getId());
						
					}
				}
				System.out.printf("%d - %d -> %d\n", start, start + chunk, c1);
				System.out.println(c1);
			}
		}
	*/

	}

}
