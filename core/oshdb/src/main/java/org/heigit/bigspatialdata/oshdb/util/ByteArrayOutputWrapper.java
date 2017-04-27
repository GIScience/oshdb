package org.heigit.bigspatialdata.oshdb.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.protobuf.CodedOutputStream;

public class ByteArrayOutputWrapper {

	ByteArrayOutputStream bos = new ByteArrayOutputStream();
	CodedOutputStream cos = CodedOutputStream.newInstance(bos);

	public void writeUInt32(int value) throws IOException{
		cos.writeUInt32NoTag(value);
	}
	
	public void writeSInt32(int value) throws IOException{
		cos.writeSInt32NoTag(value);
	}
	
	public void writeUInt64(long value) throws IOException{
		cos.writeUInt64NoTag(value);
	}
	
	public void writeSInt64(long value) throws IOException{
		cos.writeSInt64NoTag(value);
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
	
	public byte[] toByteArray() throws IOException{
		cos.flush();
		return bos.toByteArray();
	}	
}
