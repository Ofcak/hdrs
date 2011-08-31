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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.TripleDeserializer;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.compression.BlockDecompressor;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.compression.Compression.ALGORITHM;
import de.hpi.fgis.hdrs.tio.SeekableTripleSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;

/**
 * Reader for triple files.
 * 
 * Note files can only be read in state OPEN.  In this state, the triple file block index
 * is loaded into memory.  Thus, the file must be opened using open() before reading.
 * 
 * This class also contains the logic to transfer-out a triple file block-by-block
 * during a segment transfer.  (See TripleFileStreamer)
 * 
 * @author daniel.hefenbrock
 *
 */
public class TripleFileReader implements SeekableTripleSource, Closeable {
  
  final File file;
  RandomAccessFile raf = null;
    
  protected BlockIndex blockIndex = null;
  protected FileTrailer trailer = null;
  
  
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  
  public TripleFileReader(String filename) {
    file = new File(filename);
  }
  
  public TripleFileReader(File file) {
    this.file = file;
  }
  
  public TripleFileReader(TripleFileWriter writer) {
    if (!writer.isClosed()) {
      throw new IllegalStateException("Writer must be closed first.");
    }
    file = new File(writer.getFileName());
  }
  
  
  @Override
  public Triple.COLLATION getOrder() {
    return trailer.order;
  }
  
  Compression.ALGORITHM getCompression() {
    return trailer.compression;
  }
  
  /**
   * Get file info for this triple file.  
   * @return  File info for triple file, or null if file is not open.
   * @throws IOException
   */
  public TripleFileInfo getFileInfo() {
    if (!isOpen()) {
      return null;
    }
    return new TripleFileInfo(
        trailer.order, 
        trailer.compression,
        this.getClass() == HalfFileReader.class,
        getFileSize(),
        getUncompressedFileSize(),
        getUncompressedDataSize(),
        getNumberOfTriples(),
        file.getAbsolutePath(),
        trailer.indexSize);
  }
  
  
  public String getFileName() {
    return file.getName();
  }
  
  
  public long getFileSize() {
    return file.length();
  }
  
  
  public long getUncompressedFileSize() {
    if (!isOpen()) {
      return -1;
    }
    return trailer.uncompressedSize;
  }
  
  
  protected long getDataSize() {
    return trailer.indexOffset;
  }
  
  
  public long getUncompressedDataSize() {
    return trailer.dataSize;
  }
  
  
  public long getNumberOfTriples() {
    return trailer.nTriples;
  }
  
  
  protected BlockIndex getIndex() {
    return blockIndex;
  }
  
  
  public void delete() throws IOException {
    close();
    if (!file.delete()) {
      throw new IOException("Couldn't delete triple file.");
    }
  }
  
  
  /**
   * Read file info and block index;
   * @throws IOException 
   */
  public synchronized void open() throws IOException {
    if (isOpen()) {
      return;
    }
    // open file for random access
    raf = new RandomAccessFile(file, "r");
    // seek to file trailer
    raf.seek(raf.length() - FileTrailer.SIZE);
    // read file trailer
    trailer = new FileTrailer();
    trailer.read(raf);
    // read block index
    blockIndex = readIndex();
    // we should be at the file trailer now
    if (raf.getFilePointer() != raf.length() - FileTrailer.SIZE) {
      throw new IOException("Wrong file info offset.");
    }
  }
  
  synchronized void open(FileTrailer trailer, BlockIndex index) throws FileNotFoundException {
    if (isOpen()) {
      return;
    }
    // open file for random access
    raf = new RandomAccessFile(file, "r");
    
    this.trailer = trailer;
    this.blockIndex = index;
  }
  
  synchronized BlockIndex readIndex() throws IOException {
    // seek to block index
    raf.seek(trailer.indexOffset);
    // read offsets
    int olen = (int) ((Long.SIZE / Byte.SIZE) * trailer.blockCount);
    ByteBuffer offsetBuffer = ByteBuffer.allocate(olen);
    raf.readFully(offsetBuffer.array(), 0, olen);
    // read indexed triples
    int tlen = (int) (raf.length() - FileTrailer.SIZE - trailer.indexOffset - olen);
    ByteBuffer triplesBuffer = ByteBuffer.allocate(tlen);
    raf.readFully(triplesBuffer.array(), 0, tlen);
    // decompress index
    if (Compression.ALGORITHM.NONE != trailer.compression) {
      BlockDecompressor decompressor = trailer.compression.getDecompressor();
      ByteBuffer uncompressed = ByteBuffer.allocate(trailer.indexSize - olen);
      decompressor.uncompress(triplesBuffer.array(), 0, triplesBuffer.limit(), 
          uncompressed.array(), 0);
      triplesBuffer = uncompressed;
    }
    return BlockIndex.readIndex(trailer.blockCount, offsetBuffer, triplesBuffer, trailer.order);
  }
  
  public int getIndexSize() {
    return trailer.indexSize;
  }
  
  public boolean isOpen() {
    return null != trailer && null != blockIndex;
  }
  
  @Override
  public void close() throws IOException {
    if (!isOpen()) {
      return;
    }
    // make sure no one is currently reading a block
    lock.writeLock().lock();
    
    raf.close();
    trailer = null;
    blockIndex = null;
    
    lock.writeLock().unlock();
  }
  
  @Override
  public String toString() {
    return "TripleFile.Reader(filename = " + file.getAbsolutePath() +
      (null != trailer ? ", "+trailer.toString() : "") +
      (null != blockIndex ? ", " + blockIndex.toString() : "") + ")";
  }
  
  int getBlockCount() {
    lock.readLock().lock();
    int count = blockIndex.getBlockCount();
    lock.readLock().unlock();
    return count;
  }
  
  
  protected TripleScanner getScanner(int startBlockId) throws IOException {
    return new FileScanner(this, startBlockId);
  }
  
  /**
   * Get a scanner that is positioned to the beginning of the file.
   * @return A scanner
   * @throws IOException
   */
  @Override
  public TripleScanner getScanner() throws IOException {
    if (!isOpen()) {
      throw new FileClosedException();
    }
    return getScanner(0);
  }
  
  
  //TODO refactor
  @Override
  public TripleScanner getScanner(Triple pattern) throws IOException  {
    if (!isOpen()) {
      throw new FileClosedException();
    }
    int startBlock = lookupBlock(pattern);
    if (-2 == startBlock) {
      // pattern is after this file
      return null;
    } else if (-1 == startBlock) {
      // pattern is before this file --> start art block 0
      startBlock = 0;
    }
    TripleScanner scanner = getScanner(startBlock);
    // now seek scanner into position
    scanner.seek(pattern);
    return scanner;
  }
  
  //TODO refactor
  @Override
  public TripleScanner getScannerAt(Triple pattern) throws IOException  {
    if (!isOpen()) {
      throw new FileClosedException();
    }
    int startBlock = lookupBlock(pattern);
    if (0 > startBlock) {
      // pattern is out of the range of this file
      return null;
    }
    TripleScanner scanner = getScanner(startBlock);
    // now seek scanner into position
    if (scanner.seek(pattern)) {
      return scanner;
    }
    // pattern is not in file.
    scanner.close();
    return null;
  }
  
  // TODO refactor
  @Override
  public TripleScanner getScannerAfter(Triple pattern) throws IOException {
    
    lock.readLock().lock(); 
    
    if (!isOpen()) {
      throw new FileClosedException();
    }
    int startBlock = lookupBlock(pattern);
    
    lock.readLock().unlock(); 
    
    if (-1 == startBlock) {
      // pattern is before file, thus we need to go to the first triple!!!
      startBlock = 0;
    } else if (-2 == startBlock) {
      // pattern is after file --> out of range
      return null;
    }
    TripleScanner scanner = getScanner(startBlock);
    // now seek scanner into position
    if (scanner.seekAfter(pattern)) {
      return scanner;
    }
    // pattern is not in file.
    scanner.close();
    return null;
  }
  
  
  protected int lookupBlock(Triple pattern) {
    return blockIndex.lookup(pattern);
  }
  
  
  public Triple getSplitTriple() throws IOException {
    if (!blockIndex.getMidTriple().isMagic()) {
      return blockIndex.getMidTriple();
    }
    // triple was clipped.  we have to fetch it from disk
    TripleScanner blockScanner = new TripleDeserializer(
        getOrder(), readBlock(blockIndex.getMidBlock(), true));
    blockScanner.next();
    return blockScanner.pop();
  }

  
  ByteBuffer readBlock(int blockId, boolean inflate) throws IOException {
    
    // make sure no one closes the file while we are reading in a block from disk
    lock.readLock().lock(); 
    
    if (!isOpen()) {
      // file is closed already
      lock.readLock().unlock();
      throw new FileClosedException();
    }
    
    long offset = blockIndex.getBlockOffset(blockId);
    int length;
    if (blockIndex.getBlockCount() == 1+blockId) {
      // last block
      length = (int) (trailer.indexOffset - offset);
    } else {
      length = (int) (blockIndex.getBlockOffset(1+blockId) - offset);
    }
    
    ByteBuffer buffer = ByteBuffer.allocate(length);
    synchronized (raf) {
      raf.seek(offset);
      raf.readFully(buffer.array(), 0, length);
    }
    
    // make copies... this could be nulled on close() by another thread
    Compression.ALGORITHM compression = trailer.compression;
    
    lock.readLock().unlock();
    
    if (inflate && Compression.ALGORITHM.NONE != compression) {
      BlockDecompressor decompressor = compression.getDecompressor();
      ByteBuffer uncompressed = ByteBuffer.allocate(
          decompressor.uncompressedLength(buffer.array(), 0, length));
      decompressor.uncompress(buffer.array(), 0, length, uncompressed.array(), 0);
      buffer = uncompressed;
    }
    
    if (!isOpen()) {
      // file was closed
      throw new FileClosedException();
    }
    
    return buffer;
  }
  
  /**
   * Scan a triple file.
   * 
   * @author daniel.hefenbrock
   *
   */
  static class FileScanner extends TripleScanner {

    final TripleFileReader reader;
    final int blockCount;
    final Triple.COLLATION order;
    
    int currentBlockId;
    TripleScanner blockScanner = null;
    
    FileScanner(TripleFileReader reader, int blockId) {
      this.reader = reader;
      currentBlockId = blockId;
      blockCount = reader.getBlockCount(); // getBlockCount() is synchronized
      order = reader.getOrder(); // synchronized too
    }
    
    @Override
    protected Triple nextInternal() throws IOException {
      // next is null
      if (null == blockScanner) {
        // we need to fetch the first block
        if (blockCount < currentBlockId) {
          // this can happen in halffile
          return null;
        }
        blockScanner = new TripleDeserializer(order, 
            reader.readBlock(currentBlockId, true));
      } 
      if (!blockScanner.next()) {
        // current block is done
        if (currentBlockId+1 == blockCount) {
          // no more blocks!
          return null;
        }
        // read next block
        blockScanner = new TripleDeserializer(order, 
            reader.readBlock(++currentBlockId, true));
        if (!blockScanner.next()) {
          return null; // empty block?  strange...
        }
      }     
      // read next triple
      return blockScanner.pop();
    }
    
    @Override
    public void close() throws IOException {
      blockScanner = null;
    }

    @Override
    public COLLATION getOrder() {
      return reader.getOrder();
    }
    
    @Override
    public String toString() {
      return "TripleFileScanner(" + reader + ")";
    }
    
  }
  
  
  public FileStreamer getStreamer() {
    return new TripleFileStreamer(this);
  }
  
  
  /**
   * Used for triple file transfers.
   * 
   * @author daniel.hefenbrock
   *
   */
  static class TripleFileStreamer implements FileStreamer {

    protected final TripleFileReader reader;
    protected int currentBlockId = 0;
    
    TripleFileStreamer(TripleFileReader reader) {
      this.reader = reader;
    }

    @Override
    public ALGORITHM getCompression() {
      return reader.getCompression();
    }    

    @Override
    public COLLATION getOrder() {
      return reader.getOrder();
    }

    @Override
    public FileBlock nextBlock() throws IOException {
      if (currentBlockId < reader.getBlockCount()) {      
        FileBlock block = new FileBlock(
            reader.readBlock(currentBlockId, false),
            reader.getIndex().getIndexedTriple(currentBlockId));
        currentBlockId++;
        return block;       
      }
      return null;
    }

    @Override
    public Triple getLastTriple() {
      return reader.getIndex().getLastTriple();
    }

    @Override
    public long getDataSize() {
      return reader.getUncompressedDataSize();
    }

    @Override
    public long getNumberOfTriples() {
      return reader.getNumberOfTriples();
    }
 
  }
  
  
  public static class FileClosedException extends IOException {
    private static final long serialVersionUID = 194170855694648889L;
    
    public FileClosedException() {
      super();
    }
    
  }
  
}
