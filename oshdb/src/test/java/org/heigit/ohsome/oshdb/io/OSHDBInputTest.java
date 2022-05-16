package org.heigit.ohsome.oshdb.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.CodedOutputStream;
import java.io.IOException;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;

class OSHDBInputTest {
  private byte[] bytes = new byte[128];

  @Property
  void uInt32(@ForAll @IntRange(min = 0, max = Integer.MAX_VALUE) int i) throws IOException {
    var cos = CodedOutputStream.newInstance(bytes);
    cos.writeUInt32NoTag(i);
    var input = OSHDBInput.wrap(bytes);
    assertEquals(i, input.readUInt32());
  }

  @Property
  void sInt32(@ForAll @IntRange(min = Integer.MIN_VALUE, max = Integer.MAX_VALUE) int i) throws IOException {
    var cos = CodedOutputStream.newInstance(bytes);
    cos.writeSInt32NoTag(i);
    var input = OSHDBInput.wrap(bytes);
    assertEquals(i, input.readSInt32());
  }

  @Property
  void uInt64(@ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long i) throws IOException {
    var cos = CodedOutputStream.newInstance(bytes);
    cos.writeUInt64NoTag(i);
    var input = OSHDBInput.wrap(bytes);
    assertEquals(i, input.readUInt64());
  }

  @Property
  void sInt64(@ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long i) throws IOException {
    var cos = CodedOutputStream.newInstance(bytes);
    cos.writeSInt64NoTag(i);
    var input = OSHDBInput.wrap(bytes);
    assertEquals(i, input.readSInt64());
  }
}
