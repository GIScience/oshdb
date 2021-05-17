package org.heigit.ohsome.oshdb.util.bytearray;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import org.junit.Test;

/**
 *  General {@link ByteArrayOutputWrapper} test case.
 */
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
  public void testReadU32() throws IOException {
    assertReadU32(bytes(0x00), 0);
    assertReadU32(bytes(0x01), 1);
    assertReadU32(bytes(0x7f), 127);
    assertReadU32(bytes(0x80, 0x01), 128);
    assertReadU32(bytes(0xff, 0x01), 255);
    assertReadU32(bytes(0xa2, 0x74), 14882);
    assertReadU32(bytes(0xff, 0xff, 0xff, 0xff, 0x07), Integer.MAX_VALUE);
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
  public void testReadS32() throws IOException {
    assertReadS32(bytes(0x00), 0);
    assertReadS32(bytes(0x01), -1);
    assertReadS32(bytes(0x02), 1);
    assertReadS32(bytes(0xfe, 0x01), 127);
    assertReadS32(bytes(0xfd, 0x01), -127);
    assertReadS32(bytes(0x80, 0x02), 128);
    assertReadS32(bytes(0xff, 0x01), -128);
    assertReadS32(bytes(0xfe, 0x03), 255);
    assertReadS32(bytes(0xfd, 0x03), -255);
    assertReadS32(bytes(0xc4, 0xe8, 0x01), 14882);
    assertReadS32(bytes(0xc3, 0xe8, 0x01), -14882);
    assertReadS32(bytes(0xfe, 0xff, 0xff, 0xff, 0x0f), Integer.MAX_VALUE);
    assertReadS32(bytes(0xff, 0xff, 0xff, 0xff, 0x0f), Integer.MIN_VALUE);
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
  public void testReadU64() throws IOException {
    assertReadU64(bytes(0x00), 0);
    assertReadU64(bytes(0x01), 1);
    assertReadU64(bytes(0x7f), 127);
    assertReadU64(bytes(0x80, 0x01), 128);
    assertReadU64(bytes(0xff, 0x01), 255);
    assertReadU64(bytes(0xa2, 0x74), 14882);
    assertReadU64(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f), Long.MAX_VALUE);
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

  @Test
  public void testReadS64() throws IOException {
    assertReadS64(bytes(0x00), 0);
    assertReadS64(bytes(0x01), -1);
    assertReadS64(bytes(0x02), 1);
    assertReadS64(bytes(0xfe, 0x01), 127);
    assertReadS64(bytes(0xfd, 0x01), -127);
    assertReadS64(bytes(0x80, 0x02), 128);
    assertReadS64(bytes(0xff, 0x01), -128);
    assertReadS64(bytes(0xfe, 0x03), 255);
    assertReadS64(bytes(0xfd, 0x03), -255);
    assertReadS64(bytes(0xc4, 0xe8, 0x01), 14882);
    assertReadS64(bytes(0xc3, 0xe8, 0x01), -14882);
    assertReadS64(bytes(0xfe, 0xff, 0xff, 0xff, 0x0f), Integer.MAX_VALUE);
    assertReadS64(bytes(0xff, 0xff, 0xff, 0xff, 0x0f), Integer.MIN_VALUE);
    assertReadS64(bytes(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01),
        Long.MAX_VALUE);
    assertReadS64(bytes(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01),
        Long.MIN_VALUE);
  }

  @Test
  public void testWriteByteArray() throws IOException {
    var bao = new ByteArrayOutputWrapper(1024);
    var bytes = bytes(0xc4, 0xe8, 0x01);
    bao.writeByteArray(bytes);
    assertEquals(3, bao.length());
    assertArrayEquals(bytes, Arrays.copyOf(bao.array(), 3));

    bao.reset();
    bao.writeByteArray(bytes, 1, 1);
    assertEquals(1, bao.length());
    assertEquals((byte) 0xe8, bao.array()[0]);
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
    assertEquals(bs.length, bao.length());
    assertArrayEquals(bs, Arrays.copyOf(bao.array(), bs.length));
  }

  private void assertReadU32(byte[] bytes, int expect) throws IOException {
    var baw = new ByteArrayWrapper(bytes, 0, bytes.length);
    var actual = baw.readU32();
    assertEquals(expect, actual);
  }

  private void assertReadS32(byte[] bytes, int expect) throws IOException {
    var baw = new ByteArrayWrapper(bytes, 0, bytes.length);
    var actual = baw.readS32();
    assertEquals(expect, actual);
  }

  private void assertReadU64(byte[] bytes, long expect) throws IOException {
    var baw = new ByteArrayWrapper(bytes, 0, bytes.length);
    var actual = baw.readU64();
    assertEquals(expect, actual);
  }

  private void assertReadS64(byte[] bytes, long expect) throws IOException {
    var baw = new ByteArrayWrapper(bytes, 0, bytes.length);
    var actual = baw.readS64();
    assertEquals(expect, actual);
  }

  private static byte[] bytes(int... b) {
    var bytes = new byte[b.length];
    for (int i = 0; i < b.length; i++) {
      bytes[i] = (byte) b[i];
    }
    return bytes;
  }
}
