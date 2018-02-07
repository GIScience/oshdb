package org.heigit.bigspatialdata.oshdb.util.byteArray;

import java.io.IOException;

import com.google.protobuf.CodedOutputStream;

import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class ByteArrayOutputWrapper {

	final FastByteArrayOutputStream bos;
	final CodedOutputStream cos;
	
	
	public ByteArrayOutputWrapper(int bufferSize){
	  bos = new FastByteArrayOutputStream(bufferSize);
	  cos = CodedOutputStream.newInstance(bos,bufferSize);
  }
	
	public ByteArrayOutputWrapper(){
	  this(256);
	}

	public void writeUInt32(int value) throws IOException{
		cos.writeUInt32NoTag(value);
	}
	
	public void writeSInt32(int value) throws IOException{
		cos.writeSInt32NoTag(value);
	}
	
	public int writeSInt32Delta(int value, int last) throws IOException{
	  writeSInt32(value - last);
	  return value;
	}
	
	public void writeUInt64(long value) throws IOException{
		cos.writeUInt64NoTag(value);
	}
	
	public long writeUInt64Delta(long value, long last) throws IOException {
	  final long delta = value - last;
	  if(delta < 0)
	    throw new IllegalArgumentException("writeUInt64Delta with negative delta("+delta+")");
	  writeUInt64(delta);
	  return value;
	}
	
	public void writeSInt64(long value) throws IOException{
		cos.writeSInt64NoTag(value);
	}
	
	public long writeSInt64Delta(long value, long last) throws IOException {
	  writeSInt64(value - last);
	  return value;
	}
	
	public void writeByte(byte value) throws IOException {
		cos.writeRawByte(value);	
	}
	
	public void writeByteArray(byte[] value) throws IOException{
		cos.writeRawBytes(value);
	}
	
	public void writeByteArray(final byte[] value, int offset, int length) throws IOException{
	  cos.writeRawBytes(value, offset, length);
	}
	
	public void reset(){
	  bos.reset();
	}
	
	public int length() throws IOException{
	  cos.flush();
	  return bos.length;
	}
	
	public byte[] array() throws IOException{
	  cos.flush();
	  return bos.array;
	}
	

	
	public FastByteArrayOutputStream getByteArrayStream() throws IOException{
		cos.flush();
		return bos;
	}	
}
