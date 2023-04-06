package org.heigit.ohsome.oshdb.util.celliterator;

import com.google.common.collect.Streams;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGrid;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.util.CellId;

/**
 * A source of OSH entities.
 */
public interface OSHEntitySource {

  /**
   * Returns a stream of OSH entities.
   *
   * @return a stream of OSH entities
   */
  Stream<? extends OSHEntity> getData();

  /**
   * Returns the bounding box of the entities returned by `getData()`.
   *
   * <p>By convention this bbox must contain the bboxes of all OSH entities returned by this source.
   * It should be the minimal bbox encompassing all of the entities.</p>
   *
   * @return A bounding box enclosing all OSH entities of this source
   */
  OSHDBBoundingBox getBoundingBox();

  /**
   * A helper method which transforms a grid cell to an OSH entity source.
   *
   * @param cell A grid cell containing OSH entities
   * @return A source object which will return the entities of the given grid cell and its bbox
   */
  static OSHEntitySource fromGridOSHEntity(GridOSHEntity cell) {
    return new OSHEntitySource() {
      @Override
      public Stream<? extends OSHEntity> getData() {
        return Streams.stream(cell.getEntities());
      }

      @Override
      public OSHDBBoundingBox getBoundingBox() {
        return XYGrid.getBoundingBox(new CellId(cell.getLevel(), cell.getId()), true);
      }
    };
  }
}
