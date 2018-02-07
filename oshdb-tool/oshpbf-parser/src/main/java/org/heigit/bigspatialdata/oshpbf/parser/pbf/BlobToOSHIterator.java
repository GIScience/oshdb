package org.heigit.bigspatialdata.oshpbf.parser.pbf;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Entity;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;

import crosby.binary.Osmformat;

public class BlobToOSHIterator implements Iterator<Osh> {

  private final PbfBlob blob;
  private final OsmPrimitveBlockIterator primitiveIterator;

  private Entity nextEntity = null;
  private Osh next = null;;

  public BlobToOSHIterator(PbfBlob blob, Osmformat.PrimitiveBlock block, boolean skipFirst) {
    this.blob = blob;
    this.primitiveIterator = new OsmPrimitveBlockIterator(blob.pos, block,
        EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION));
    if (!primitiveIterator.hasNext())
      return;

    nextEntity = primitiveIterator.next();
    if (nextEntity.getVersion() != 1 && blob.isFirstBlob && skipFirst) {
      // skip next versions of the same id
      System.out.println("skip " + nextEntity);
      final long skip = nextEntity.getId();
      nextEntity = null;
      while (primitiveIterator.hasNext()) {
        Entity e = primitiveIterator.next();
        if (e.getId() != skip) {
          nextEntity = e;
          break;
        }
      }
    }
    next = getNext();
  }

  @Override
  public boolean hasNext() {
    return (next != null);
  }

  @Override
  public Osh next() {
    if (!hasNext())
      throw new NoSuchElementException();
    Osh result = next;
    next = getNext();
    return result;
  }

  public Osh peek() {
    return next;
  }

  private Osh getNext() {
    if (nextEntity == null)
      return null;

    final OSMType lastType = nextEntity.getType();
    final long lastId = nextEntity.getId();

    List<Entity> versions = new ArrayList<>();
    versions.add(nextEntity);

    nextEntity = null;
    while (primitiveIterator.hasNext()) {
      final Entity e = primitiveIterator.next();
      final long id = e.getId();
      final OSMType type = e.getType();
      if (lastType != type) {
        System.err.printf("diffrent types in one blob (id:%d/%d)(type:%s/%s) at blob:%d%n", lastId, id, lastType,
            type, primitiveIterator.getBlockStartPosition());
        break;
      }
      if (id != lastId) {
        if (!blob.overSoftLimit) {
          nextEntity = e;
        }
        return new Osh(versions.get(0).getVersion() == 1, versions,blob.pos);
      }

      versions.add(e);
    }
    return new Osh(false, versions,blob.pos);
  }
}
