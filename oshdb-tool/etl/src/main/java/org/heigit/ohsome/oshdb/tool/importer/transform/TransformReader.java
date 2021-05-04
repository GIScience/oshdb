package org.heigit.ohsome.oshdb.tool.importer.transform;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.tool.importer.transform.oshdb.TransformOSHWay;
import org.heigit.ohsome.oshdb.tool.importer.util.ZGrid;

public class TransformReader {
  public final Path path;
  private final RandomAccessFile raf;
  private final long end;
  protected final FileChannel channel;

  private final ByteBuffer header = ByteBuffer.allocateDirect(8 + 4 + 4);

  protected long pos = 0;

  public long cellId;
  protected int size;
  protected int bytes;

  private TransformReader(Path path) throws IOException {
    this.path = path;
    this.raf = new RandomAccessFile(path.toFile(), "r");
    this.end = raf.length();
    this.channel = raf.getChannel();

    readHeader();
  }

  protected void skip() {
    pos += bytes;
  }

  protected void readHeader() throws IOException {
    header.clear();
    channel.read(header, pos);
    pos += header.capacity();

    header.flip();
    this.cellId = header.getLong();
    this.size = header.getInt();
    this.bytes = header.getInt();
  }

  public long getCellId() {
    return cellId;
  }

  public boolean hasNext() {
    return (pos + bytes) < end;
  }

  public void next() throws IOException {
    skip();
    readHeader();
  }

  @Override
  public String toString() {
    return String.format("%d:%d %d, (#%d  %d)", (pos - 16), end, cellId, size, bytes);
  }


  public static class NodeReader extends TransformReader {
    public NodeReader(Path path) throws IOException {
      super(path);
    }

    public Set<OSHNode> get() throws IOException {
      final ByteBuffer data = ByteBuffer.allocateDirect(bytes);
      channel.read(data, pos);
      data.flip();

      Set<OSHNode> ret = new TreeSet<>((a, b) -> Long.compare(a.getId(), b.getId()));
      while (data.hasRemaining()) {
        int length = data.getInt();
        byte[] content = new byte[length];
        data.get(content);
        OSHNode node = OSHNodeImpl.instance(content, 0, length);
        System.out.println(node.getId());
        ret.add(node);
      }
      ret.stream().filter(node -> node.getId() == 553542L).forEach(System.out::println);
      return ret;
    }
  }

  public static class NodeReaders {
    PriorityQueue<NodeReader> queue = new PriorityQueue<>((a, b) -> {
      int c = ZGrid.ORDER_DFS_TOP_DOWN.compare(a.cellId, b.cellId);
      if (c != 0) {
        return c;
      }
      return a.path.compareTo(b.path);
    });

    public NodeReaders(NodeReader... readers) {
      for (NodeReader r : readers) {
        queue.add(r);
      }
    }

    public long cellId() {
      return queue.peek().cellId;
    }

    public Set<OSHNode> get() throws IOException {
      List<NodeReader> readers = new ArrayList<>(queue.size());
      NodeReader reader = queue.poll();
      readers.add(reader);
      System.out.println(reader.cellId + " " + reader.path);
      Set<OSHNode> nodes = reader.get();
      nodes.stream().filter(node -> node.getId() == 553542L).forEach(System.out::println);
      while (!queue.isEmpty() && (queue.peek().cellId == reader.cellId)) {
        nodes.addAll(queue.peek().get());
        readers.add(queue.poll());
      }
      readers.stream().filter(NodeReader::hasNext).forEach(r -> {
        try {
          r.next();
          queue.add(r);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      });
      return nodes;
    }

    public boolean hasNext() {
      return !queue.isEmpty();
    }

  }


  public static class WayReader extends TransformReader {
    public WayReader(Path path) throws IOException {
      super(path);
    }

    public Set<TransformOSHWay> get() throws IOException {
      final ByteBuffer data = ByteBuffer.allocateDirect(bytes);
      channel.read(data, pos);
      data.flip();


      Set<TransformOSHWay> ret = new TreeSet<>((a, b) -> Long.compare(a.getId(), b.getId()));
      while (data.hasRemaining()) {
        int length = data.getInt();
        byte[] content = new byte[length];
        data.get(content);
        TransformOSHWay e = TransformOSHWay.instance(content, 0, length);
        ret.add(e);

      }
      return ret;
    }
  }



  public static void load(List<Grid<TransformOSHWay>> zoomLevel, int zoom,
      Long2ObjectMap<OSHNode> nodes) {
    Grid<TransformOSHWay> grid = zoomLevel.get(zoom);
    if (grid != null) {
      System.out.println("remove " + zoom);
      zoomLevel.set(zoom, null);
    }
  }

  public static class Grid<T> {
    public final long cellId;
    public final Set<T> entities;
    // public final Roaring64NavigableMap refMap;
    public final Set<Long> refMapSet;

    private Grid(long cellId, Set<T> entities, Set<Long> refMapSet) {
      this.cellId = cellId;
      this.entities = entities;
      this.refMapSet = refMapSet;
    }

    public static <T> Grid<T> of(long cellId, Set<T> ways, Set<Long> refMapSet) {
      return new Grid<T>(cellId, ways, refMapSet);
    }
  }
}
