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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleDeserializer;
import de.hpi.fgis.hdrs.compression.BlockCompressor;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.TripleScanner;

/**
 * A triple file reader that offers a half-file view on a triple files.
 * This is used after a segment split in order to avoid copying files.
 * 
 * @author daniel.hefenbrock
 *
 */
public class HalfFileReader extends TripleFileReader {

  public static enum HALF {
    TOP,
    BOTTOM
  }
  
  
  // first triple of the bottom half
  private final Triple splitTriple;
  private final HALF half;
  
  private int splitBlockId;
  
  
  public HalfFileReader(TripleFileReader file, Triple split, HALF half) {
    this(file.file, split, half);
  }
  
  public HalfFileReader(String filename, Triple split, HALF half) {
    this(new File(filename), split, half);
  }
  
  public HalfFileReader(File file, Triple split, HALF half) {
    super(file);
    splitTriple = split;
    this.half = half;
  }

  
  public boolean isTopHalf() {
    return HALF.TOP == half;
  }
  
  
  @Override
  public long getFileSize() {
    if (!isOpen()) {
      return -1;
    }
    return super.getFileSize() - (super.getDataSize() - getDataSize());
  }
  
  
  @Override
  public long getUncompressedFileSize() {
    if (!isOpen()) {
      return -1;
    }
    return super.getUncompressedFileSize() / 2;
  }
  
  
  @Override
  protected long getDataSize() {
    if (!isOpen()) {
      return -1;
    }
    if (isTopHalf()) {
      if (getBlockCount() <= splitBlockId+1) {
        return super.getDataSize();
      } else {
        return blockIndex.getBlockOffset(splitBlockId+1);
      }
    } else {
      if (getBlockCount() > splitBlockId) {
        return super.getDataSize() - blockIndex.getBlockOffset(splitBlockId);
      } else {
        return 0;
      }
    }
  }
  
  
  @Override
  public long getUncompressedDataSize() {
    if (!isOpen()) {
      return -1;
    }
    // yea, not cool, but there's not much else we can do.
    return super.getUncompressedDataSize() / 2;
  }
  
  
  @Override
  public long getNumberOfTriples() {
    // same here.
    return super.getNumberOfTriples() / 2;
  }
  
  
  @Override
  public Triple getSplitTriple() {
    return splitTriple;
  }
  
  
  @Override
  public synchronized void open() throws IOException {
    super.open();
    splitBlockId = super.lookupBlock(splitTriple);
    if (-1 == splitBlockId) {
      // the splitTriple is before the first block of this file 
      splitBlockId = 0;
    } else if (-2 == splitBlockId) {
      // the splitTriple is after the last block of this file
      splitBlockId = getBlockCount() + 1;
    }
  }
  
  
  protected int getSplitBlockId() {
    return splitBlockId;
  }
  
  
  @Override
  protected int lookupBlock(Triple pattern) {
    int blockId = super.lookupBlock(pattern);
    if (0 > blockId) {
      // pattern is out of the range of this file
      return blockId;
    }
    if (isTopHalf()) {
      if (0 <= getOrder().comparator().compare(pattern, splitTriple)) {
        // splitTriple <= pattern ... pattern is in bottom half
        return -2; // pattern after file
      }
    } else {
      if (0 < getOrder().comparator().compare(splitTriple, pattern)) {
        // pattern < splitTriple ... pattern is in top half
        return -1;
      }
    }
    return blockId;
  }
  
  
  @Override
  public TripleScanner getScanner() throws IOException {
    if (!isOpen()) {
      throw new FileClosedException();
    }
    TripleScanner scanner = getScanner(isTopHalf() ? 0 : splitBlockId);
    if (!isTopHalf()) {
      scanner.seek(splitTriple);
    }
    return scanner;
  }
  
  @Override
  protected TripleScanner getScanner(int startBlockId) throws IOException {
    return new SplitFileScanner(this, startBlockId);
  }
  
  
  static class SplitFileScanner extends TripleFileReader.FileScanner {

    SplitFileScanner(HalfFileReader reader, int blockId) {
      super(reader, blockId);
    }
    
    private HalfFileReader getHalfReader() {
      return (HalfFileReader) reader;
    }
    
    @Override
    protected Triple nextInternal() throws IOException {
      Triple next = super.nextInternal();
      if (null != next
          && getHalfReader().isTopHalf() 
          && currentBlockId >= getHalfReader().getSplitBlockId()) {
        // are we still in the top half?
        if (0 <= getOrder().comparator().compare(next, getHalfReader().getSplitTriple())) {
          // pattern is in bottom half
          return null; // pattern after file
        }
      }
      return next;
    }
    
  }
  
  
  public FileStreamer getStreamer() {
    return new SplitFileStreamer(this);
  }
  
  /**
   * Streamer for triple file transfer.
   * 
   * @author daniel.hefenbrock
   *
   */
  static class SplitFileStreamer extends TripleFileReader.TripleFileStreamer {

    private Triple lastTopHalfTriple = null;
    
    SplitFileStreamer(HalfFileReader reader) {
      super(reader);
      if (!getHalfReader().isTopHalf()) {
        currentBlockId = getHalfReader().getSplitBlockId();
      }
    }
    
    private HalfFileReader getHalfReader() {
      return (HalfFileReader) reader;
    }
    
    @Override
    public FileBlock nextBlock() throws IOException {
      if (reader.getBlockCount() > currentBlockId
          && currentBlockId == getHalfReader().getSplitBlockId()) {
        // get block (uncompressed)
        ByteBuffer buffer = reader.readBlock(currentBlockId, true);
        // seek to split triple
        TripleDeserializer deserializer = new TripleDeserializer(getOrder(), buffer);
        while (true) {
          if (!deserializer.next()) {
            // everything's smaller than split triple
            if (getHalfReader().isTopHalf()) {
              // entire block goes to upper half
              return super.nextBlock();
            } else {
              // lower half: return next block
              currentBlockId++;
              return nextBlock();
            }
          }
          Triple t = deserializer.pop();
          int cmp = getOrder().comparator().compare(
              getHalfReader().getSplitTriple(), t);
          if (0 >= cmp) {
            // found it
            if (null == lastTopHalfTriple) {
              // this is the first triple of this block.
              if (getHalfReader().isTopHalf()) {
                return null;
              } else {
                // entire block goes to lower half
                return super.nextBlock();
              }
            }
            // buffer is now positioned right after the split triple.
            break;
          } 
          lastTopHalfTriple = t;
          buffer.mark();
        }
        lastTopHalfTriple = lastTopHalfTriple.copy();
        
        if (getHalfReader().isTopHalf()) {
          // take first half
          // set buffer to point at split triple.
          buffer.reset();
          buffer.flip();
        } else {
          // take second half
          // rewrite last triple (we can't simply split a block somewhere)
          buffer = deserializer.slice();
          if (null == buffer) {
            throw new IOException("unable to slice triple buffer");
          }
        }
        
        // deflate
        if (getCompression() != Compression.ALGORITHM.NONE) {
          BlockCompressor compressor = getCompression().getCompressor();
          ByteBuffer compressed = ByteBuffer.allocate(
              compressor.maxCompressedLength(buffer.limit()));
          int compressedLen = compressor.compress(buffer.array(), buffer.arrayOffset(), 
              buffer.limit(), compressed.array(), 0);
          compressed.limit(compressedLen);
          buffer = compressed;        
        }
        FileBlock block = new FileBlock(buffer, 
            getHalfReader().isTopHalf() 
              ? reader.getIndex().getIndexedTriple(currentBlockId)
              : getHalfReader().getSplitTriple());
        currentBlockId++;
        return block;
      }
      if (getHalfReader().isTopHalf() 
          && getHalfReader().getSplitBlockId() < currentBlockId) {
        return null;
      }
      return super.nextBlock();
    }
    
    @Override
    public Triple getLastTriple() {
      if (getHalfReader().isTopHalf()) {
        return lastTopHalfTriple;
      }
      return super.getLastTriple();
    }
    
  }
  
  
  @Override
  public String toString() {
    return "HalfFileReader(TripleFileReader = " + super.toString()
        + ", splitTriple = " + splitTriple
        + ", splitBlockId = " + splitBlockId
        + ", half = " + half
    		+ ")";
  }
  
  
}
