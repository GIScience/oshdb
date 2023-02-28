package org.heigit.ohsome.oshdb.store;

import java.util.Objects;
import org.heigit.ohsome.oshdb.osm.OSMType;

public class OSHData {
   private final OSMType type;
   private final long id;

   public OSHData(OSMType type, long id) {
      this.type = type;
      this.id = id;
   }

   public OSMType getType() {
      return type;
   }

   public long getId() {
      return id;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof OSHData)) {
         return false;
      }
      OSHData oshData = (OSHData) o;
      return id == oshData.id && type == oshData.type;
   }

   @Override
   public int hashCode() {
      return Objects.hash(type, id);
   }

   @Override
   public String toString() {
      return "OSHData " + type + "/" + id;
   }
}
