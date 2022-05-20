package org.heigit.ohsome.oshdb.util;

import java.util.ArrayList;
import java.util.Map;

public class LCS {
  enum Action {
    TAKE, SKIP, ADD, UPDATE
  }

  public static class Sequence extends ArrayList<Map.Entry<Action, Integer>> {
    private void add(Action action, int size) {
      add(Map.entry(action, size));
    }
  }

  public static Sequence lcs(int[] prev, int[] next) {
    return lcs(prev, prev.length, next, next.length);
  }

  public static Sequence lcs(int[] prev, int prevSize, int[] next, int nextSize) {
    var commonPrefix = 0;
    var minSize = Math.min(prevSize, nextSize);
    while (commonPrefix < minSize && prev[commonPrefix] == next[commonPrefix]) {
      commonPrefix++;
    }

    var sequence = new Sequence();
    if (prevSize == nextSize && commonPrefix == minSize) {
      return sequence;
    }

    if (commonPrefix == prevSize) {
      sequence.add(Action.TAKE, commonPrefix);
      sequence.add(Action.ADD, nextSize - commonPrefix);
      return sequence;
    }

    if (commonPrefix > 0) {
      sequence.add(Action.TAKE, commonPrefix);
    }

    if (commonPrefix == nextSize) {
      return sequence;
    }


    var commonSuffix = 0;
    var prevPos = prevSize - 1;
    var nextPos = nextSize - 1;

    while (prevPos >= commonPrefix && nextPos >= commonPrefix && prev[prevPos] == next[nextPos]) {
      commonSuffix++;
      prevPos--;
      nextPos--;
    }

    var prevDiff = prevSize - commonPrefix - commonSuffix;
    var nextDiff = nextSize - commonPrefix - commonSuffix;
    var matrixLength = (prevDiff + 1) * (nextDiff + 1);
    var matrix = new int[matrixLength];

    var x = 1;
    var y = prevDiff + 1;
    var z = y + 1;

    // Bottom up dynamic programming
    // https://www.ics.uci.edu/~eppstein/161/960229.html
    var l = (nextDiff) * (prevDiff + 1) - 2;
    for (nextPos = (commonPrefix + nextDiff) - 1; nextPos >= commonPrefix; nextPos--) {
      for (prevPos = (commonPrefix + prevDiff) - 1; prevPos >= commonPrefix; prevPos--) {
        if (prev[prevPos] == next[nextPos]) {
          matrix[l] = matrix[l + z] + 1;
        } else {
          matrix[l] = Math.max(matrix[l + x], matrix[l + y]);
        }
        l--;
      }
      l--; // skip additional zero column;
    }

    // extract action sequence from matrix
    // https://www.ics.uci.edu/~eppstein/161/960229.html
    var prevSizeWithoutSuffix = prevSize - commonSuffix;
    var nextSizeWithoutSuffix = nextSize - commonSuffix;

    var take = 0;
    var add = 0;
    var skip = 0;
    var update = 0;
    prevPos = nextPos = commonPrefix;
    var subMatrixLength = (nextDiff) * (prevDiff + 1) - 1;
    l = 0;
    while (l < subMatrixLength) {
      if (prev[prevPos] != next[nextPos]) {
        if (take > 0) {
          sequence.add(Action.TAKE, take);
          take = 0;
        }
        if (matrix[l + x] > matrix[l + y]) {
          skip++;
          prevPos++;
          l += x;
        } else {
          add++;
          nextPos++;
          l += y;
        }
      } else {
        if (skip > 0) {
          update = Math.min(skip, add);
          if (update > 0) {
            sequence.add(Action.UPDATE, update);
            skip -= update;
            add -= update;
          }
          if (skip > 0) {
            sequence.add(Action.SKIP, skip);
            skip = 0;
          }
        }
        if (add > 0) {
          sequence.add(Action.ADD, add);
          add = 0;
        }

        take++;
        l += z;
        prevPos++;
        nextPos++;
      }

      if (prevPos >= prevSizeWithoutSuffix || nextPos >= nextSizeWithoutSuffix) {
        break;
      }
    }

    if (take > 0) {
      sequence.add(Action.TAKE, take);
    }

    skip += prevSizeWithoutSuffix - prevPos;
    add += nextSizeWithoutSuffix - nextPos;

    if (skip > 0) {
      update = Math.min(skip, add);
      if (update > 0) {
        sequence.add(Action.UPDATE, update);
        skip -= update;
        add -= update;
      }
      if (skip > 0) {
        sequence.add(Action.SKIP, skip);
      }
    }
    if (add > 0) {
      sequence.add(Action.ADD, add);
    }

    if (commonSuffix > 0) {
      sequence.add(Action.TAKE, commonSuffix);
    }

    return sequence;
  }
}
