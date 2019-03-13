package org.heigit.bigspatialdata.oshpbf.parser.rx;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.heigit.bigspatialdata.oshpbf.parser.pbf.BlobToOSHIterator;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.PbfBlob;
import org.heigit.bigspatialdata.oshpbf.parser.util.ByteBufferBackedInputStream;

import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Fileformat;
import crosby.binary.Osmformat;
import crosby.binary.Osmformat.HeaderBlock;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;;

public class RxOshPbfReader {

  private static final byte[] SIGNATURE_OSMDATA   = { /* wire_type */10, /* stringSize */7, 79, 83, 77, 68, 97, 116, 97 };
  private static final byte[] SIGNATURE_OSMHEADER = { /* wire_type */10, /* stringSize */9, 79, 83, 77, 72, 101, 97, 100, 101, 114 };
  private static final int BLOBHEADER_SIZE_BYTES = 4;
  private static final int SIGNATURE_SIZE_BYTES = Math.max(SIGNATURE_OSMDATA.length, SIGNATURE_OSMHEADER.length);

  public static class PbfChannel {
    final RandomAccessFile raf;
    final FileChannel channel;
    final long softLimit;
    final long hardLimit;
    final long startPos;
    final boolean foundSignatur;

    ByteBuffer buffer = ByteBuffer.allocateDirect(0);

    public PbfChannel(RandomAccessFile raf, long softLimit, long hardLimit, boolean foundSignatur) throws IOException {
      this.raf = raf;
      this.softLimit = softLimit;
      this.hardLimit = hardLimit;
      this.channel = raf.getChannel();
      this.startPos = channel.position();
      this.foundSignatur = foundSignatur;

    }

    public ByteBuffer readFully(int size) throws IOException {
      if (buffer.capacity() < size) {
        // System.out.println("increase buffer capasity to " + size);
        buffer = ByteBuffer.allocateDirect(size);
      } else {
        buffer.clear();
        buffer.limit(size);
      }
      channel.read(buffer);
      buffer.rewind();
      return buffer;
    }
  }

  public static Flowable<PbfBlob> readBlob(Path pbfPath, long pos, long softLimit, long hardLimit) {
    final Callable<PbfChannel> initialState = openPbfAndSeekFirstBlockStart(pbfPath, pos, softLimit, hardLimit);
    final BiFunction<PbfChannel, Emitter<PbfBlob>, PbfChannel> generator = readBlobFromChannel();
    final Consumer<? super PbfChannel> disposeState = closePbf();
    Flowable<PbfBlob> blobFlow = Flowable.generate( //
        initialState, generator, //
        disposeState); //
    return blobFlow.subscribeOn(Schedulers.io());
  }

  public static Flowable<Osh> readOsh(Path pbfPath, long pos, long softLimit, long hardLimit) {
    return readOsh(pbfPath, pos, softLimit, hardLimit, header -> {});
  }
  public static Flowable<Osh> readOsh(Path pbfPath, long pos, long softLimit, long hardLimit, Consumer<HeaderBlock> header) {
    final int cpus = Runtime.getRuntime().availableProcessors();
    final int maxConcurrency = cpus - 1;
    final int prefetch = 4;

    Flowable<Osh> oshFlow = readBlob(pbfPath, pos, softLimit, hardLimit)
        .doOnNext(blob -> {
          if(blob.isHeader())
            header.accept(blob.getHeaderBlock());
        })
        .filter(PbfBlob::isData)
        .observeOn(Schedulers.computation())
        .concatMapEager(
            blob -> Flowable.just(blob).subscribeOn(Schedulers.computation()).map(b -> blobToOSHItr(b, false)),
            maxConcurrency, prefetch)
        .flatMap(Flowable::fromIterable);
    oshFlow = new OshMerger(oshFlow);

    return oshFlow;
  }

  public static Iterable<Osh> blobToOSHItr(PbfBlob blob, boolean skipFirst) throws InvalidProtocolBufferException {
    return new Iterable<Osh>() {
      final Osmformat.PrimitiveBlock block = blob.getPrimitivBlock();

      @Override
      public Iterator<Osh> iterator() {
        return new BlobToOSHIterator(blob, block, skipFirst);
      }
    };
  }

  private static Callable<PbfChannel> openPbfAndSeekFirstBlockStart(Path pbfPath, long pos, long softLimit,
      long hardLimit) {
    return () -> {
      RandomAccessFile raf = new RandomAccessFile(pbfPath.toFile(), "r");
      final long sl, hl;

      hl = (hardLimit <= 0) ? raf.getChannel().size() : hardLimit;
      sl = Math.min(((softLimit <= 0) ? raf.getChannel().size() : softLimit), hl);

      if (seekStartPos(raf, pos, sl))
        return new PbfChannel(raf, sl, hl, true);
      return new PbfChannel(raf, -1, -1, false);
    };
  }

  private static boolean seekStartPos(RandomAccessFile raf, long pos, long limit) throws IOException {
    final FileChannel ch = raf.getChannel();
    ch.position(pos);
    final long readUntilHeader = seekBlobHeaderStart(Channels.newInputStream(ch), limit);
    if (readUntilHeader == -1)
      return false;

    final long startPosition = pos + readUntilHeader;
    ch.position(startPosition);
    return true;
  }

  public static long seekBlobHeaderStart(final InputStream is, final long limit) throws IOException {
    long totalBytesRead = 0;
    final byte[] pushBackBytes = new byte[BLOBHEADER_SIZE_BYTES + SIGNATURE_SIZE_BYTES];
    PushbackInputStream pushBackStream = new PushbackInputStream(is, pushBackBytes.length);
    for (int i = 0; i < BLOBHEADER_SIZE_BYTES; i++) {
      pushBackBytes[i] = (byte) pushBackStream.read();
      totalBytesRead++;
    }

    int nextByte = pushBackStream.read();
    totalBytesRead++;
    int val = 0;
    while (nextByte != -1) {
      if ((val < SIGNATURE_OSMDATA.length && SIGNATURE_OSMDATA[val] == nextByte)
          || (val < SIGNATURE_OSMHEADER.length && SIGNATURE_OSMHEADER[val] == nextByte)) {

        pushBackBytes[BLOBHEADER_SIZE_BYTES + val] = (byte) nextByte;

        if (val == SIGNATURE_OSMDATA.length - 1 || val == SIGNATURE_OSMHEADER.length - 1) {
          // Full OSMHeader\Data SIGNATURE is found.
          pushBackStream.unread(pushBackBytes, 0, BLOBHEADER_SIZE_BYTES + val + 1);
          totalBytesRead -= BLOBHEADER_SIZE_BYTES + val + 1;
          return totalBytesRead;
        }
        val++;
      } else if (val != 0) {
        // break
        if (limit > 0 && totalBytesRead > limit) {
          return -1;
        }

        val = 0;
        if (SIGNATURE_OSMDATA[val] == nextByte || SIGNATURE_OSMHEADER[val] == nextByte) {
          pushBackBytes[BLOBHEADER_SIZE_BYTES + val] = (byte) nextByte;
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
      totalBytesRead++;
    }

    // we reach the end of stream and found no signature
    return -1;
  }

  private static BiFunction<PbfChannel, Emitter<PbfBlob>, PbfChannel> readBlobFromChannel() {
    return (pbf, output) -> {
      if (!pbf.foundSignatur) {
        output.onError(new Throwable("no OSMData/Header Signatur within limit!"));
        return pbf;
      }

      try {
        if (pbf.channel.position() >= pbf.hardLimit) {
          output.onComplete();
          return pbf;
        }

        final long blobPos = pbf.channel.position();
        final boolean overSoftlimit = blobPos > pbf.softLimit;

        ByteBuffer buffer = pbf.readFully(BLOBHEADER_SIZE_BYTES);
        final int headerSize = buffer.getInt();

        buffer = pbf.readFully(headerSize);
        final Fileformat.BlobHeader header = Fileformat.BlobHeader.PARSER
            .parseFrom(new ByteBufferBackedInputStream(buffer));

        buffer = pbf.readFully(header.getDatasize());
        final Fileformat.Blob blob = Fileformat.Blob.PARSER.parseFrom(new ByteBufferBackedInputStream(buffer));

        PbfBlob pbfBlob = new PbfBlob(blobPos, header, blob, blobPos == pbf.startPos, overSoftlimit);
        output.onNext(pbfBlob);
        if (overSoftlimit) {
          output.onComplete();
        }
      } catch (Exception error) {
        output.onError(error);
      }
      return pbf;
    };
  }

  private static Consumer<PbfChannel> closePbf() {
    return (input) -> {
      try {
        input.buffer = null;
        if (input.channel.isOpen())
          input.channel.close();

        input.raf.close();
      } catch (IOException io) {

      }
    };
  }
}
