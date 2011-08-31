/**
 * Copyright 2011 Daniel Hefenbrock
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.hpi.fgis.hdrs.triplefile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleDeserializer;

/**
 * Triple file block index.
 * 
 * @author daniel.hefenbrock
 *
 */
class BlockIndex {
  
  int blockCount;
  Triple.COLLATION order;
  long[] blockOffsets;
  Triple[] indexedTriples;
  
  Triple lastTriple = null;
  
  // roughly track size in bytes of this index (memory footprint)
  int size = 0;
  
  private BlockIndex(int indexSize, Triple.COLLATION order) {
    blockCount = 0;
    this.order = order;
    blockOffsets = new long[indexSize];
    indexedTriples = new Triple[indexSize];
  }
  
  /**
   * Used to create the block index.
   * @param size 
   */
  void addBlock(final long offset, final Triple indexedTriple) {
    blockOffsets[blockCount] = offset;
    indexedTriples[blockCount] = indexedTriple;
    blockCount++;
  }
  
  
  long getBlockOffset(int blockId) {
    return blockOffsets[blockId];
  }
  
  Triple getIndexedTriple(int blockId) {
    return indexedTriples[blockId];
  }
  
  int getBlockCount() {
    return blockOffsets.length;
  }
  
  Triple getLastTriple() {
    return lastTriple;
  }
  
  Triple getMidTriple() {
    return indexedTriples[getMidBlock()];
  }
  
  int getMidBlock() {
    return indexedTriples.length/2;
  }
  
  /**
   * Search for the first block containing 
   * @return block id if matching triple in block, or -1 if before, or
   * -2 of after file
   */
  int lookup(Triple triple) {
    int blockId = binarySearch(indexedTriples, triple, order.comparator());
    // -1 --> before first block.  in case of a pattern we need to check
    // if the pattern matches the first triple
    if (-1 == blockId) {
      if (order.comparator().match(indexedTriples[0], triple)) {
        return 0;
      }
      // before first block
      return -1;
    }
    // check if triple or pattern is after file
    if ((indexedTriples.length-1 == blockId
          && 1 == order.comparator().compare(triple, lastTriple))) {
      return -2;
    }
    return blockId;
  }
  
  static int binarySearch(Triple[] index, Triple triple, Comparator<Triple> comparator) {
    int low = 0;
    int high = index.length - 1;
    while (low <= high) {
      int mid = (low+high) >>> 1;
      int cmp = comparator.compare(triple, index[mid]);
      // key lives above the midpoint
      if (cmp > 0)
        low = mid + 1;
      // key lives below the midpoint
      else if (cmp < 0)
        high = mid - 1;
      // found it.  however, there could be more in front of it!
      else
        // magic triple means triple was clipped. back off one block.
        return index[mid].isMagic() ? mid - 1 : mid;
    }
    return low < high ? low : high;
  }
  
  
  static ByteBuffer writeIndexOffsets(final List<Long> indexOffsets) throws IOException {
          
    ByteBuffer out = ByteBuffer.allocate((Long.SIZE / Byte.SIZE) * indexOffsets.size());
    
    for (int i=0; i<indexOffsets.size(); ++i) {
      out.putLong(indexOffsets.get(i).longValue());
    }
    return out;
  }
  
  static BlockIndex readIndex(final int indexSize,
      final ByteBuffer offsetBuffer,
      final ByteBuffer triplesBuffer,
      final Triple.COLLATION order) throws IOException {
    
    BlockIndex index = new BlockIndex(indexSize, order);
    TripleDeserializer deserializer = new TripleDeserializer(order, triplesBuffer);
    
    for (int i=0; i<indexSize; ++i) {
      long offset = offsetBuffer.getLong();
      deserializer.next();
      Triple t = deserializer.pop();
      index.addBlock(offset, t);
      index.size += Long.SIZE / Byte.SIZE + t.estimateSize();
    }
    index.size = triplesBuffer.capacity() 
        + indexSize * (Long.SIZE/Byte.SIZE + Triple.HEADER_SIZE);
    
    deserializer.next();
    index.lastTriple = deserializer.pop();
    
    return index;
  }
  
  @Override
  public String toString() {
    return "BlockIndex(blockCount = " + blockCount
      + ", size ~" + size + " bytes" 
      + ", first triple = " + indexedTriples[0]
      + ", last triple = " + lastTriple
      + ")";
  }
  
}
