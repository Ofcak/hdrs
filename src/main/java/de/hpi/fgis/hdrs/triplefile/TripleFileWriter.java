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

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleBuffer;
import de.hpi.fgis.hdrs.TripleSerializer;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.compression.BlockCompressor;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.OrderedTripleSink;

/**
 * This class writes triples to a file on disk.  Triples are written in order,
 * that is, they are expected to be passed to the Writer in order.  The Writer
 * splits triples into blocks and adds a block index to the end of the file.
 * This block index can then be used to search for triples when reading the
 * file, this way reducing I/O.
 * 
 * A triple file on disk conceptually looks like this:
 * 
 * <pre>
 *                           .-----------------------.
 *                           |                       |
 *                           V                       |
 * ---------------------------------------------------------
 * |         |     |         |             |               |
 * | Block 1 | ... | Block n | Block Index | File Trailer  |
 * |         |     |         |             |               |
 * ---------------------------------------------------------
 * </pre>
 * 
 * Block 1 through n and the block index are compressed.
 * 
 * At the end of the file there is a fixed size trailer that among other
 * things contains pointers (offsets) to the block index and file info.
 * 
 * @author hefenbrock
 *
 */
public class TripleFileWriter implements OrderedTripleSink, Closeable {

  // how many bytes of a triple to index at most.
  // triples longer than this will be clipped when going into the index
  public static final int INDEXED_BYTES = 256;
  
  public static final int DEFAULT_BLOCKSIZE = 128*1024; // before: 64K
  public static final int BUFFER_SIZE = 32*1024;
  
  private final String filename;
  
  // Stream to keep track of global file offsets etc.
  protected OutputStream out;
  
  // Compression
  protected Compression.ALGORITHM compressionAlgorithm;
  private BlockCompressor compressor = null;
  
  private ByteBuffer currentBlockBuffer = null;
  
  private TripleSerializer currentBlock = null;
  
  // write buffer
  private final List<ByteBuffer> writes = new ArrayList<ByteBuffer>();
  private int bufferSize = 0;
  
  // collation order for this file
  Triple.COLLATION order;
  
  // uncompressed file offset
  protected long uncompressedBytes = 0;
  
  // compressed file offset
  protected long compressedBytes = 0;
  
  // block size in bytes
  private int blockSizeBytes;
  
  // first triple of the current block
  private Triple blockFirstTriple = null;
  
  // offset where the current block begins
  private long blockOffset;
  
  // previously written triple
  protected Triple lastTriple = null;
  
  // total number of triples in this file
  protected int tripleCount = 0;
  
  protected long dataSize = 0;
  
  // trailer is set after file is closed
  private FileTrailer trailer = null;
    
  // used to record information for the block index
  protected TripleBuffer indexedTriples;
  protected ArrayList<Long> indexOffsets = new ArrayList<Long>();
  
  
  public TripleFileWriter(final String filename) throws FileNotFoundException {
    this(filename, Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
  }
  
  public TripleFileWriter(final String filename, final Triple.COLLATION order,
      final Compression.ALGORITHM compression) throws FileNotFoundException {
    this(filename, order, DEFAULT_BLOCKSIZE, compression);
  }
  
  public TripleFileWriter(final String filename, final Triple.COLLATION order,
      final int blockSize, Compression.ALGORITHM compression) throws FileNotFoundException {
    out = new FileOutputStream(filename);
    this.filename = filename;
    this.order = order;
    blockSizeBytes = blockSize;
    compressionAlgorithm = compression;
    if (compress()) {
      compressor = compression.getCompressor();
    }
    indexedTriples = new TripleBuffer(order, ByteBuffer.allocate(64 * 1024));
  }
  
  protected boolean compress() {
    return Compression.ALGORITHM.NONE != compressionAlgorithm;
  }
  
  public void appendTriple(final Triple t) throws IOException {
    if (null != lastTriple) {
// sanity check disabled for performance      
//      if (0 < order.comparator().compare(lastTriple, t)) {
//        throw new IOException("Triple ordering constraint violated wnile writing TripleFile.");
//      }
    } else {
      // first triple in this file.  start first block.
      newBlock();
    }
    
    if (!currentBlock.add(t)) {
      finishBlock();
      newBlock();
      if (!currentBlock.add(t)) {
        //throw new IOException("cannot write triple larger than block size");
        currentBlockBuffer = ByteBuffer.allocate(1 + t.serializedSize());
        currentBlock = new TripleSerializer(order, currentBlockBuffer);
        currentBlock.add(t);
      }
    }
    lastTriple = t;
    
    // first triple of this block?
    if (null == blockFirstTriple) {
      blockFirstTriple = t; 
    }
    
    dataSize += t.serializedSize();
    tripleCount++;
  }
  
  
  @Override
  public boolean add(Triple triple) throws IOException {
    appendTriple(triple);
    return true;
  }
  
  @Override
  public COLLATION getOrder() {
    return order;
  }
  
  protected long fileOffset() {
    return compress() ? compressedBytes : uncompressedBytes;
  }
  
  private void newBlock() throws IOException {
    blockFirstTriple = null;
    
    currentBlockBuffer = ByteBuffer.allocate(blockSizeBytes);
    currentBlock = new TripleSerializer(order, currentBlockBuffer);
    
    // bytes written so far in this file
    blockOffset = fileOffset();
  }
  
  protected void finishBlock() throws IOException {
    if (null != blockFirstTriple) {  // block could be empty.
      if (INDEXED_BYTES < blockFirstTriple.bufferSize()) {
        blockFirstTriple = blockFirstTriple.clip(order, INDEXED_BYTES);
      }
      indexedTriples.add(blockFirstTriple);
      indexOffsets.add(Long.valueOf(blockOffset));
        
      uncompressedBytes += currentBlockBuffer.position();
    
      // now compress and flush
      if (compress()) {
        ByteBuffer compressed = ByteBuffer.allocate(
            compressor.maxCompressedLength(currentBlockBuffer.position()));
        int compressedLen = compressor.compress(
            currentBlockBuffer.array(), 0, currentBlockBuffer.position(), 
            compressed.array(), 0);
        compressed.position(compressedLen);
        compressedBytes += compressedLen;
        write(compressed);
      } else {
        write(currentBlockBuffer);
      }
    }
    currentBlockBuffer = null;
  }
  
  private void write(ByteBuffer buf) throws IOException {
    writes.add(buf);
    bufferSize += buf.position();
    if (bufferSize >= BUFFER_SIZE) {
      flush();
    }
  }
  
  private void flush() throws IOException {
    // flush
    for (ByteBuffer write : writes) {
      out.write(write.array(), 0, write.position());
    }
    writes.clear();
    bufferSize = 0;
  }
  
  @Override
  public void close() throws IOException {
    close(false);
  }
  
  public TripleFileReader closeAndOpen() throws IOException {
    BlockIndex index = close(true);
    if (null == index) {
      return null;
    }
    TripleFileReader reader = new TripleFileReader(filename);
    reader.open(trailer, index);
    return reader;
  }
  
  public BlockIndex close(boolean getIndex) throws IOException {
    
    if (0 == tripleCount) {
      // no triples written at all?  do nothing.
      out.close();
      File file = new File(filename);
      file.delete();
      return null;
    }
    
    finishBlock();
    flush();
    
    trailer = new FileTrailer();
    trailer.order = order;
    trailer.compression = compressionAlgorithm;
    
    // write block index to file
    trailer.indexOffset = fileOffset();
    
    ByteBuffer bufferOffsets = BlockIndex.writeIndexOffsets(indexOffsets);
    out.write(bufferOffsets.array(), bufferOffsets.arrayOffset(), bufferOffsets.limit());
    trailer.indexSize = bufferOffsets.capacity();
    
    indexedTriples.add(lastTriple);
    ByteBuffer bufferTriples = indexedTriples.getBuffer();
    trailer.indexSize += bufferTriples.position() - bufferTriples.arrayOffset();
    
    uncompressedBytes += trailer.indexSize;
    
    if (compress()) {
      ByteBuffer compressed = ByteBuffer.allocate(
          compressor.maxCompressedLength(bufferTriples.position() - bufferTriples.arrayOffset()));
      int compressedLen = compressor.compress(
          bufferTriples.array(), bufferTriples.arrayOffset(), bufferTriples.position(), 
          compressed.array(), 0);
      compressedBytes += compressedLen;
      out.write(compressed.array(), 0, compressedLen);
    } else {
      out.write(bufferTriples.array(), bufferTriples.arrayOffset(), bufferTriples.position());
    }
    
    // store number of blocks
    trailer.blockCount = indexOffsets.size();
    
    uncompressedBytes += FileTrailer.SIZE;
    if (compress()) {
      compressedBytes += FileTrailer.SIZE;
    }
    trailer.uncompressedSize = uncompressedBytes;
    
    trailer.dataSize = dataSize;
    trailer.nTriples = tripleCount;
    
    // write the trailer
    trailer.write(new DataOutputStream(out));
    
    out.close();
    
    if (getIndex) {
      bufferOffsets.rewind();
      bufferTriples.rewind();
      return BlockIndex.readIndex(trailer.blockCount, bufferOffsets, bufferTriples, order);
    }
    return null;
  }
  
  public boolean isClosed() {
    return null != trailer;
  }
  
  public String getFileName() {
    return filename;
  }
  
  public TripleFileInfo getFileInfo() {
    if (!isClosed()) {
      throw new IllegalStateException("getFileInfo() called before closing file");
    }
    return new TripleFileInfo(trailer, filename, 
        !compress() ? uncompressedBytes : compressedBytes);
  }
  
  public String toString() {
    return "Writer (file = " + filename 
      + ", blockSize = " + blockSizeBytes 
      + ", compression = " + compressionAlgorithm + ")";
  }

}
