package org.heigit.bigspatialdata.oshpbf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.*;
import java.util.stream.StreamSupport;

import crosby.binary.Osmformat;
import crosby.binary.Osmformat.PrimitiveBlock;

public class OsmPbfBlocks implements Iterable<Osmformat.PrimitiveBlock> {

	
	private final InputStream in;
	private final long length;
	
	public OsmPbfBlocks(final InputStream in, final long length){
		this.in = in;
		this.length =  length;
	}
	
	@Override
	public Iterator<PrimitiveBlock> iterator() {
		try {
			return new OsmPrimitiveBlockIterator(in, length);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return Collections.emptyIterator();
	}
	
	public Stream<Osmformat.PrimitiveBlock> stream(){		
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(),Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.NONNULL), false);
	}

}
