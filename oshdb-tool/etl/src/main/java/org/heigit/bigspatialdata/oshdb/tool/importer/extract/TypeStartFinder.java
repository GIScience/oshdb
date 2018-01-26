package org.heigit.bigspatialdata.oshdb.tool.importer.extract;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshpbf.parser.pbf.PbfBlob;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;

import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Osmformat;

public class TypeStartFinder {
  
  public static OsmPbfMeta getMetaData(Path pbf) throws InvalidProtocolBufferException{
    OsmPbfMeta meta = new OsmPbfMeta();
   
    Path metaPath = pbf.getParent().resolve(pbf.getFileName().toString()+".meta");
    
    if(Files.exists(metaPath)){
      try(DataInputStream input = new DataInputStream(new FileInputStream(metaPath.toFile()))){
         meta.pbf = pbf;
         meta.nodeStart = input.readLong();
         meta.nodeEnd = input.readLong();
         meta.wayStart = input.readLong();
         meta.wayEnd = input.readLong();
         meta.relationStart = input.readLong();
         meta.relationEnd = input.readLong();
         return meta;
      } catch (IOException e) {
        System.err.println(e);
      }
    }
    
    long fileSize = pbf.toFile().length();
    PbfBlob header = RxOshPbfReader.readBlob(pbf, 0, fileSize, -1).blockingFirst();
    
    PbfBlob node = RxOshPbfReader.readBlob(pbf, 0, fileSize, -1).filter(PbfBlob::isData).blockingFirst();  
    PbfBlob way = findWay(pbf);
    PbfBlob relation = findRelation(pbf, way.pos+1);
    
    meta.pbf = pbf;
    meta.nodeStart = node.pos;
    meta.nodeEnd = way.pos;
    meta.wayStart = way.pos;
    meta.wayEnd = relation.pos;
    meta.relationStart = relation.pos;
    meta.relationEnd = fileSize;
    
    try(DataOutputStream output = new DataOutputStream(new FileOutputStream(metaPath.toFile()))){
      output.writeLong(meta.nodeStart);
      output.writeLong(meta.nodeEnd);
      output.writeLong(meta.wayStart);
      output.writeLong(meta.wayEnd);
      output.writeLong(meta.relationStart);
      output.writeLong(meta.relationEnd);
    } catch (IOException e) {
      System.err.println(e);
    }
    return meta;    
  }

  public static PbfBlob findWay(Path pbf) throws InvalidProtocolBufferException {
    long fileSize = pbf.toFile().length();

    long low = 0;
    long high = fileSize;

    while (high >= low) {
      long middle = (low + high) / 2;

      Iterator<PbfBlob> blob = RxOshPbfReader.readBlob(pbf, middle, -1, -1).take(2).blockingIterable().iterator();

      if (blob.hasNext()) {
        PbfBlob b = blob.next();
        OSMType type = getType(b);

        if (type == OSMType.NODE) {
          if (!blob.hasNext()) {
            System.out.println("Found nothing");
            return null;
          }
          b = blob.next();
          type = getType(b);
          if(type == OSMType.WAY){
            System.out.println("Found Way at "+b.pos);
            return b;
          }else if (type == OSMType.NODE){
            low = middle +1;
          }
        }else {
          high = middle + 1;
        }
      } else {
        System.out.println("Found nothing");
        return null;
      }

    }

    return null;
  }
  
  public static PbfBlob findRelation(Path pbf, long startPos) throws InvalidProtocolBufferException {
    long fileSize = pbf.toFile().length();

    long low = startPos;
    long high = fileSize;

    while (high >= low) {
      long middle = (low + high) / 2;

      Iterator<PbfBlob> blob = RxOshPbfReader.readBlob(pbf, middle, -1, -1).take(2).blockingIterable().iterator();

      if (blob.hasNext()) {
        PbfBlob b = blob.next();
        OSMType type = getType(b);

        if (type == OSMType.WAY) {
          if (!blob.hasNext()) {
            System.out.println("Found nothing");
            return null;
          }
          b = blob.next();
          type = getType(b);
          if(type == OSMType.RELATION){
            System.out.println("Found Relation at "+b.pos);
            return b;
          }else if (type == OSMType.WAY){
            low = middle +1;
          }
        }else {
          high = middle + 1;
        }
      } else {
        System.out.println("Found nothing");
        return null;
      }

    }

    return null;
  }

  public static OSMType getType(PbfBlob blob) throws InvalidProtocolBufferException {
    Osmformat.PrimitiveGroup group = blob.getPrimitivBlock().getPrimitivegroup(0);
    if (group.hasDense() || group.getNodesCount() > 0)
      return OSMType.NODE;
    if (group.getWaysCount() > 0)
      return OSMType.WAY;
    if (group.getRelationsCount() > 0)
      return OSMType.RELATION;
    return OSMType.UNKNOWN;

  }

}
