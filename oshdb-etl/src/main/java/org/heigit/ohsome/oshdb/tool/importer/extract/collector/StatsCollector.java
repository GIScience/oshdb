package org.heigit.ohsome.oshdb.tool.importer.extract.collector;

import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.GEOM_PRECISION;

import crosby.binary.Osmformat.HeaderBBox;
import crosby.binary.Osmformat.HeaderBlock;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshpbf.parser.osm.v06.Entity;
import org.heigit.ohsome.oshpbf.parser.osm.v06.Node;
import org.heigit.ohsome.oshpbf.parser.rx.Osh;

public class StatsCollector {

  private long[] nodes = new long[5];
  private long[] ways = new long[5];
  private long[] relations = new long[5];
  private long[] unknown = new long[5];

  private long minLon = Long.MAX_VALUE;
  private long minLat = Long.MAX_VALUE;
  private long maxLon = Long.MIN_VALUE;
  private long maxLat = Long.MIN_VALUE;

  private long minTs = Long.MAX_VALUE;
  public long maxTs = Long.MIN_VALUE;

  public HeaderBlock header = null;
  private Path pbf = null;

  public StatsCollector(Path pbf) {
    this.pbf = pbf;
  }

  /**
   * Adds Header Block.
   *
   * @param header HeaderBlock
   */
  public void addHeader(HeaderBlock header) {
    this.header = header;

    nodes[3] = Long.MAX_VALUE;
    nodes[4] = Long.MIN_VALUE;
    ways[3] = Long.MAX_VALUE;
    ways[4] = Long.MIN_VALUE;
    relations[3] = Long.MAX_VALUE;
    relations[4] = Long.MIN_VALUE;
    unknown[3] = Long.MAX_VALUE;
    unknown[4] = Long.MIN_VALUE;

  }

  /**
   * Add OSH Entity.
   *
   * @param osh Entity
   */
  public void add(Osh osh) {
    OSMType type = osh.getType();

    long[] entity;
    switch (type) {
      case NODE:
        entity = nodes;
        break;
      case WAY:
        entity = ways;
        break;
      case RELATION:
        entity = relations;
        break;
      default:
        entity = unknown;
    }

    int highesVersion = Integer.MIN_VALUE;
    boolean alive = false;

    for (Entity e : osh.getVersions()) {

      minTs = Math.min(minTs, e.getTimestamp());
      maxTs = Math.max(maxTs, e.getTimestamp());

      if (e.getVersion() > highesVersion) {
        highesVersion = e.getVersion();
        alive = e.isVisible();
      }

      if (!e.isVisible()) {
        continue;
      }

      if (type == OSMType.NODE) {
        Node n = (Node) e;
        minLon = Math.min(minLon, n.getLongitude());
        minLat = Math.min(minLat, n.getLatitude());
        maxLon = Math.max(maxLon, n.getLongitude());
        maxLat = Math.max(maxLat, n.getLatitude());
      }

    }

    entity[0]++;
    entity[1] += osh.getVersions().size();
    if (alive) {
      entity[2]++;
    }

    entity[3] = Math.min(entity[3], osh.getId());
    entity[4] = Math.max(entity[4], osh.getId());
  }

  /**
   * Prints the collected Stats to given PrintStream.
   *
   * @param out PrintStream
   */
  public void print(PrintStream out) {
    if (pbf != null) {
      out.println("file.name=" + pbf.getFileName());
    }
    if (header != null) {
      HeaderBBox bbox = header.getBbox();
      out.printf("header.bbox=%d,%d,%d,%d%n", bbox.getLeft(), bbox.getBottom(), bbox.getRight(),
          bbox.getTop());
      out.println("header.source=" + header.getSource());
      out.println("header.generator=" + header.getWritingprogram());
      out.println("header.osmosis_replication_base_url=" + header.getOsmosisReplicationBaseUrl());
      out.println("header.osmosis_replication_sequence_number="
          + header.getOsmosisReplicationSequenceNumber());
      out.println(
          "header.osmosis_replication_timestamp=" + header.getOsmosisReplicationTimestamp());
    }

    if (minLon != Integer.MAX_VALUE) {
      out.printf("data.bbox=%.7f,%.7f,%.7f,%.7f%n", minLon * GEOM_PRECISION,
          minLat * GEOM_PRECISION, maxLon * GEOM_PRECISION,
          maxLat * GEOM_PRECISION);
    }

    out.printf("data.timerange=%s,%s%n",
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(minTs), ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(maxTs), ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

    out.println("data.nodes="
        + Arrays.stream(nodes, 1, nodes.length).collect(() -> new StringBuilder("" + nodes[0]),
            (sb, i) -> sb.append(",").append(i), (a, b) -> a.append(b)).toString());
    out.println("data.ways="
        + Arrays.stream(ways, 1, ways.length).collect(() -> new StringBuilder("" + ways[0]),
            (sb, i) -> sb.append(",").append(i), (a, b) -> a.append(b)).toString());
    out.println("data.relations=" + Arrays.stream(relations, 1, relations.length)
        .collect(() -> new StringBuilder("" + relations[0]), (sb, i) -> sb.append(",").append(i),
            (a, b) -> a.append(b))
        .toString());
    if (unknown[0] > 0) {
      out.println("data.unknown=" + Arrays.stream(unknown, 1, unknown.length)
          .collect(() -> new StringBuilder("" + unknown[0]), (sb, i) -> sb.append(",").append(i),
              (a, b) -> a.append(b))
          .toString());
    }
  }
}