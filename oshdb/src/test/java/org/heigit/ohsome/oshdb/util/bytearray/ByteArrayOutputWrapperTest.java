package org.heigit.ohsome.oshdb.util.bytearray;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import org.junit.Test;

public class ByteArrayOutputWrapperTest {


  @Test
  public void testWriteU32() throws IOException {
    var bao = new ByteArrayOutputWrapper(1024);

    assertWriteU32(bao, bytes(0x00), 0);
    assertWriteU32(bao, bytes(0x01), 1);
    assertWriteU32(bao, bytes(0x7f), 127);
    assertWriteU32(bao, bytes(0x80, 0x01), 128);
    assertWriteU32(bao, bytes(0xff, 0x01), 255);
    assertWriteU32(bao, bytes(0xa2, 0x74), 14882);
    assertWriteU32(bao, bytes(0xff, 0xff, 0xff, 0xff, 0x07), Integer.MAX_VALUE);
  }

  @Test
  public void testWriteS32() throws IOException {
    var bao = new ByteArrayOutputWrapper(1024);

    assertWriteS32(bao, bytes(0x00), 0);
    assertWriteS32(bao, bytes(0x01), -1);
    assertWriteS32(bao, bytes(0x02), 1);
    assertWriteS32(bao, bytes(0xfe, 0x01), 127);
    assertWriteS32(bao, bytes(0xfd, 0x01), -127);
    assertWriteS32(bao, bytes(0x80, 0x02), 128);
    assertWriteS32(bao, bytes(0xff, 0x01), -128);
    assertWriteS32(bao, bytes(0xfe, 0x03), 255);
    assertWriteS32(bao, bytes(0xfd, 0x03), -255);
    assertWriteS32(bao, bytes(0xc4, 0xe8, 0x01), 14882);
    assertWriteS32(bao, bytes(0xc3, 0xe8, 0x01), -14882);
    assertWriteS32(bao, bytes(0xfe, 0xff, 0xff, 0xff, 0x0f), Integer.MAX_VALUE);
    assertWriteS32(bao, bytes(0xff, 0xff, 0xff, 0xff, 0x0f), Integer.MIN_VALUE);
  }

  @Test
  public void testWriteU64() throws IOException {
    var bao = new ByteArrayOutputWrapper(1024);

    assertWriteU64(bao, bytes(0x00), 0);
    assertWriteU64(bao, bytes(0x01), 1);
    assertWriteU64(bao, bytes(0x7f), 127);
    assertWriteU64(bao, bytes(0x80, 0x01), 128);
    assertWriteU64(bao, bytes(0xff, 0x01), 255);
    assertWriteU64(bao, bytes(0xa2, 0x74), 14882);

    assertWriteU64(bao, bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f),
        Long.MAX_VALUE);
  }

  @Test
  public void testWriteS64() throws IOException {
    var bao = new ByteArrayOutputWrapper(1024);

    assertWriteS64(bao, bytes(0x00), 0);
    assertWriteS64(bao, bytes(0x01), -1);
    assertWriteS64(bao, bytes(0x02), 1);
    assertWriteS64(bao, bytes(0xfe, 0x01), 127);
    assertWriteS64(bao, bytes(0xfd, 0x01), -127);
    assertWriteS64(bao, bytes(0x80, 0x02), 128);
    assertWriteS64(bao, bytes(0xff, 0x01), -128);
    assertWriteS64(bao, bytes(0xfe, 0x03), 255);
    assertWriteS64(bao, bytes(0xfd, 0x03), -255);
    assertWriteS64(bao, bytes(0xc4, 0xe8, 0x01), 14882);
    assertWriteS64(bao, bytes(0xc3, 0xe8, 0x01), -14882);
    assertWriteS64(bao, bytes(0xfe, 0xff, 0xff, 0xff, 0x0f), Integer.MAX_VALUE);
    assertWriteS64(bao, bytes(0xff, 0xff, 0xff, 0xff, 0x0f), Integer.MIN_VALUE);

    assertWriteS64(bao, bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01),
        Long.MAX_VALUE);
    assertWriteS64(bao, bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01),
        Long.MIN_VALUE);
  }


  private void assertWriteU32(ByteArrayOutputWrapper bao, byte[] bs, int v) throws IOException {
    bao.reset();
    bao.writeU32(v);
    assertEquals(bs.length, bao.length());
    assertArrayEquals(bs, Arrays.copyOf(bao.array(), bs.length));
  }

  private void assertWriteU64(ByteArrayOutputWrapper bao, byte[] bs, long v) throws IOException {
    bao.reset();
    bao.writeU64(v);
    assertEquals(bs.length, bao.length());
    assertArrayEquals(bs, Arrays.copyOf(bao.array(), bs.length));
  }

  private void assertWriteS32(ByteArrayOutputWrapper bao, byte[] bs, int v) throws IOException {
    bao.reset();
    bao.writeS32(v);
    assertEquals(bs.length, bao.length());
    assertArrayEquals(bs, Arrays.copyOf(bao.array(), bs.length));
  }

  private void assertWriteS64(ByteArrayOutputWrapper bao, byte[] bs, long v) throws IOException {
    bao.reset();
    bao.writeS64(v);

    System.out.println("v = " + v);
    for (int i = 0; i < bao.length(); i++) {
      System.out.println(i + ": " + Integer.toHexString(bao.array()[i]));
    }

    assertEquals(bs.length, bao.length());
    assertArrayEquals(bs, Arrays.copyOf(bao.array(), bs.length));
  }

  private static byte[] bytes(int... b) {
    var bytes = new byte[b.length];
    for (int i = 0; i < b.length; i++) {
      bytes[i] = (byte) b[i];
    }
    return bytes;
  }

}
