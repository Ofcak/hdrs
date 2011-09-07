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

package de.hpi.fgis.hdrs.segment;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.LogFormatUtil;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.ipc.Writable;
import de.hpi.fgis.hdrs.tio.MergedScanner;
import de.hpi.fgis.hdrs.tio.SeekableTripleSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;
import de.hpi.fgis.hdrs.triplefile.FileBlock;
import de.hpi.fgis.hdrs.triplefile.FileImport;
import de.hpi.fgis.hdrs.triplefile.FileStreamer;
import de.hpi.fgis.hdrs.triplefile.HalfFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileImporter;
import de.hpi.fgis.hdrs.triplefile.TripleFileInfo;
import de.hpi.fgis.hdrs.triplefile.TripleFileReader;
import de.hpi.fgis.hdrs.triplefile.TripleFileWriter;

public class Segment implements TripleSink, SeekableTripleSource, Closeable {
  
  static final Log LOG = LogFactory.getLog(Segment.class);
    
  // stores and manages meta data related to this segment
  private SegmentMetaData segmentMetaData;
  

  private List<TripleFileReader> files;
  
  
  private final BlockingTripleBuffer buffer;
  
  
  private final Object flushSync = new Object();
  private final Object compactionSync = new Object();
  
  
  /**
   * A list of open scanners.  We need to keep track of all open scanners
   * in order to trigger a re-seek in case a buffer flush or a compaction
   * has happened.
   */
  private final List<SegmentScanner> scanners;
  
  
  /**
   * A segment flusher where this segment can request its buffer being
   * flushed to disk, creating a new file and freeing up memory.
   */
  private SegmentService flusher;
  
  // used to prevent this segment from enqueuing itself several times for flushing
  private volatile boolean flushRequested = false;
  
  
  private SegmentService compactor;
  
  private volatile boolean compactionRequested = false;
  
  
  private volatile boolean closed = false;
  
  
  //  Configuration
  private final SegmentConfiguration conf;
  
  
  @Override
  public String toString() {
    return "Segment(id = " + segmentMetaData.getSegmentId()
        + ", #files = " + files.size()
        + ")";
  }
  
  
  private static Segment openSegmentInternal(
      SegmentConfiguration conf, SegmentMetaData metaData) 
  throws IOException {
    return new Segment(conf, metaData);
  }
  
  private static SegmentMetaData createSegmentInternal(
      SegmentIndex index, long segmentId) 
  throws IOException {
    return SegmentMetaData.create(index, segmentId);
  }
  
  
  public static void deleteSegment(SegmentIndex index, long segmentId) 
  throws IOException {
    SegmentMetaData meta = SegmentMetaData.load(index, segmentId);
    meta.delete();
  }
  
  public static Segment openSegment(SegmentConfiguration conf,
      SegmentIndex index, long segmentId) 
  throws IOException {
    SegmentMetaData meta = SegmentMetaData.load(index, segmentId);
    return openSegmentInternal(conf, meta);
  }
  
  public static void createSegment(SegmentIndex index, long segmentId) 
  throws IOException {
    createSegmentInternal(index, segmentId);
  }
  
  public static Segment createAndOpenSegment(SegmentConfiguration conf,
      SegmentIndex index, long segmentId) 
  throws IOException {
    SegmentMetaData metaData = createSegmentInternal(index, segmentId);
    return openSegmentInternal(conf, metaData);
  }
  
  /**
   * Create a temporary segment in the temp folder.
   * For testing/benchmarking. 
   *
   */
  public static Segment createTmpSegment(final Triple.COLLATION order,
      int bufferSize, int flushThreshold) throws IOException {
    return createTmpSegment(order, new SegmentConfiguration(bufferSize, flushThreshold));
  }
  
  /**
   * Create a temporary segment in the temp folder.
   * For testing/benchmarking. 
   *
   */
  public static Segment createTmpSegment(final Triple.COLLATION order,
      SegmentConfiguration conf) throws IOException {
    // create tmp dir for segment
    final File dir = File.createTempFile("hdrs_segment", "");
    if (!dir.delete()) {
      throw new IOException("Couldn't create tmp directory.");
    }
    if (!dir.mkdir()) {
      throw new IOException("Couldn't create tmp directory.");
    }
    
    // return a dummy segment index
    Segment segment = createAndOpenSegment(conf, new SegmentIndex() {
      
      @Override
      public COLLATION getOrder() {
        return order;
      }

      @Override
      public File getSegmentDir(long segmentId) {
        return dir;
      }

      @Override
      public void dereferenceSegment(long segmentId) throws IOException {
      }

      @Override
      public void setSegmentRefCount(long segmentId, int refCount) {
      }
      
    }, 1);
    segment.takeOnline();
    return segment;
  }
  
  public static void deleteTmpSegment(Segment segment) throws IOException {
    segment.delete();
  }
  
   
  private Segment(SegmentConfiguration conf, SegmentMetaData metaData) 
  throws IOException {
    this.segmentMetaData = metaData;
    this.files = metaData.getFiles();
    buffer = new BlockingTripleBuffer(metaData.getOrder(), 
        conf.getBufferSize(), conf.getFlushThreshold());
    scanners = new ArrayList<SegmentScanner>();
    
    this.conf = conf;
    
    LOG.info("Opening segment " + this);
    for (TripleFileReader file : files) {
      file.open();
    }
  }
  
  
  public int getBufferCapacity() {
    return buffer.getCapacity();
  }
  
  public int getFlushThreshold() {
    return buffer.getFlushThreshold();
  }
  
  
  public long getId() {
    return segmentMetaData.getSegmentId();
  }
  
  /**
   * Split this segment.  
   * @param topId  Segment id of top child segment.
   * @param bottomId  Segment id of bottom child segment.
   * @return  The split triple (first triple of bottom segment), or null, if the
   *          segment cannot be split because it contains a half-file.
   * @throws IOException
   */
  public Triple split(long topId, long bottomId) throws IOException {
    LOG.info("Splitting segment " + this);
    if (isOnline()) {
      throw new IOException("Cannot split segment which is online.");
    }
    if (!canBeSplit()) {
      return null;
      //throw new IOException("Cannit split segment containing a HalfFile.");
    }
    return segmentMetaData.split(topId, bottomId);
  }
  
  
  public boolean shouldBeSplit(boolean scatter) {
    if ((scatter ? conf.getScatterThreshold() : conf.getSplitThreshold()) > getSize()) {
      return false;
    }
    return canBeSplit();
  }
  
  
  /**
   * @return  False if any half triple files are present currently.
   */
  public boolean canBeSplit() {
    synchronized (this) {
      return segmentMetaData.canBeSplit();
    }
  }
  
  
  /**
   * Get the current approximate total size of all on-disk triple files
   * contained in this segment.
   * @return The size in bytes.
   */
  public long getSize() {
    long size = 0;
    synchronized (this) {
      for (TripleFileReader file : files) {
        size += file.getFileSize();
      }
    }
    return size;
  }
  
  
  public long getUncompressedSize() {
    long size = 0;
    synchronized (this) {
      for (TripleFileReader file : files) {
        size += file.getUncompressedFileSize();
      }
    }
    return size;
  }
  
  
  public int getBufferSize() {
    return buffer.getSize();
  }
  
  
  public boolean isEmpty() {
    synchronized (this) {
      return files.isEmpty() && 0 == buffer.getSize();
    }
  }
  
  
  /**
   * Take this segment online.  This opens the segment for writes.
   * All open scanners will be invalidated.
   */
  public synchronized void takeOnline() {
    if (!isOnline()) {
      LOG.info("Segment " + this + " going online.");
      synchronized (scanners) {
        buffer.open();
        // we need to invalidate all scanners since they now also need to scan
        // the buffer.
        for (SegmentScanner scanner : scanners) {
          scanner.invalidate();
        }
      }
      // check if we need a compaction right now
      if (conf.getMinCompaction() <= files.size()) {
        // possibly... schedule another one
        compactor.request(getId());
        compactionRequested = true;
      }
    }
  }
  
  
  public boolean isOnline() {
    return !buffer.isClosed();
  }
  
  /**
   * Flushes the buffer and takes the segment offline.  The segment won't
   * be writable anymore once offline.
   * @throws IOException
   */
  public void takeOffline() throws IOException { // NO synchronized!!!
    if (isOnline()) {
      LOG.info("Segment " + this + " going offline.");
      flushBufferInternal(true);
    }
  }
  
  
  public void setSegmentFlusher(SegmentService flusher) {
    this.flusher = flusher;
  }
  
  public void setSegmentCompactor(SegmentService compactor) {
    this.compactor = compactor;
  }
  
  
  public void delete() throws IOException {
    segmentMetaData.delete();
  }
  
  
  List<TripleFileReader> getFiles() {
    return files;
  }
  
  
  private void checkFlushRequest() {
    if (null != flusher && !flushRequested && buffer.shouldFlush()) {
      synchronized (buffer) {
        if (!flushRequested && buffer.shouldFlush()) {
          flushRequested = true;
          flusher.request(getId());
        }
      }
    }
  }
  
  
  @Override
  public boolean add(Triple triple) throws IOException {
    boolean added = buffer.add(triple);
    if (added) {
      checkFlushRequest();
    }
    return added;
  }

  
  public int batchAdd(TripleScanner triples) throws IOException {
    return batchAdd(Integer.MAX_VALUE, triples);
  }
  
  
  /**
   * Adds at most nBytes bytes of triple data from the triple array provided.
   * This batch add is split internally into chunks of at most MAX_BATCH_ADD bytes.
   * 
   * @param nBytes  Number of bytes to write at most.
   * @param triples 
   * @return The number of triples that were added to this buffer.
   */
  public int batchAdd(int nBytes, TripleScanner triples) throws IOException {
    if (nBytes <= conf.getMaxBatchadd()) {
      return batchAddInternal(nBytes, triples);
    }
    // split batch into smaller batches
    int nBytesAdded = 0;
    while (triples.next() && 0 < nBytes && isOnline()) {
      int nBatchBytes;
      if (nBytes >= conf.getMaxBatchadd()) {
        nBatchBytes = conf.getMaxBatchadd();
      } else {
        nBatchBytes = nBytes;
      }
      int batchAdded = batchAddInternal(nBatchBytes, triples);
      if (0 == batchAdded && null != triples.peek()) {
        // nothing was added.  there could be an extremely large triple larger than
        // BATCH_ADD.  try to add it by itself
        if (!buffer.add(triples.peek())) {
          throw new IOException("unable to add triple of size " + triples.peek().estimateSize());
        }
        batchAdded = triples.pop().estimateSize();
      }
      nBytesAdded += batchAdded;
      nBytes -= batchAdded;
    }
    return nBytesAdded;
  }
  
  
  private int batchAddInternal(int nBytes, TripleScanner triples) throws IOException {
    int added = buffer.batchAdd(nBytes, triples);
    if (0 < added) {
      checkFlushRequest();
    }
    return added;
  }
  
  
  @Override
  public COLLATION getOrder() {
    return segmentMetaData.getOrder();
  }

  @Override
  public TripleScanner getScanner(Triple pattern) throws IOException {
    synchronized (scanners) {
      TripleScanner iscanner = getScannerInternal(pattern);
      if (null == iscanner) {
        return null;
      }
      SegmentScanner scanner = new SegmentScanner(iscanner);
      scanners.add(scanner);
      return scanner;
    } 
  }
  
  
  @Override
  public TripleScanner getScannerAt(Triple pattern) throws IOException {
    synchronized (scanners) {
      TripleScanner iscanner = getScannerAtInternal(pattern);
      if (null == iscanner) {
        return null;
      }
      SegmentScanner scanner = new SegmentScanner(iscanner);
      scanners.add(scanner);
      return scanner;
    } 
  }
  
  
  @Override
  public TripleScanner getScanner() throws IOException {
    return getScanner(null);
  }
  
  
  @Override
  public TripleScanner getScannerAfter(Triple pattern) throws IOException {
    synchronized (scanners) {
      SegmentScanner scanner = new SegmentScanner(getScannerAfterInternal(pattern));
      scanners.add(scanner);
      return scanner;
    } 
  }
  
  
  private void unregisterScanner(SegmentScanner scanner) {
    synchronized (scanners) {
      scanners.remove(scanner);
    }
  }
  
  
  public FileImport fileImport(Compression.ALGORITHM compression,
      long dataSize, long nTriples) throws IOException {
    final TripleFileImporter importer = segmentMetaData.createTripleFileImport(
        compression, dataSize, nTriples);
    return new FileImport() {

      @Override
      public void addBlocks(FileBlock[] blocks) throws IOException {
        importer.addBlocks(blocks);
      }

      @Override
      public boolean close(Triple lastTriple) throws IOException {
        if (!importer.close(lastTriple)) {
          return false;
        }
        // now open reader and commit meta data
        TripleFileReader file = new TripleFileReader(importer);
        file.open();
        synchronized (segmentMetaData) {
          segmentMetaData.addFile(file);
          segmentMetaData.prepareWrite();
          segmentMetaData.commitWrite();
          
          files = segmentMetaData.getFiles();
        }
        return true;
      }
    };
  }
  
  
  public SegmentExport export() {
    final Iterator<TripleFileReader> files;
    synchronized (segmentMetaData) {
      files = segmentMetaData.getFiles().iterator();
    }
    return new SegmentExport() {

      @Override
      public FileStreamer nextFile() {
        if (!files.hasNext()) {
          return null;
        }
        TripleFileReader file = files.next();
        return file.getStreamer();
      }
      
    };
  }
  
  
  public synchronized int getNumberOfFiles() {
    return files.size();
  }
  
  
  /**
   * Flushes the segment buffer if the resulting number of triple files
   * is less or equal to MAX_COMPACT_FILES.  Otherwise does nothing.
   * 
   * force can be used to override MAX_COMPACT_FILES.
   * 
   * @return Number of bytes flushed (-1 = no flush b/c too many files,
   *    0 == no flush b/c nothing to flush).
   */
  public int flushBuffer(boolean force) throws IOException {
    if (!force && getNumberOfFiles() >= conf.getMaxFiles()) {
      // too many files!! we cannot flush now
      return -1;
    }
    return flushBufferInternal(false);
  }
  
  /**
   * Flush the segment's buffer.
   * THIS IGNORES THE MAX FILES LIMIT.
   */
  public int flushBuffer() throws IOException {
    return flushBufferInternal(false);
  }
  
  
  /**
   * <p> Flushes the buffer of this segment to a new on-disk file.
   * 
   * <p> Called by flusher thread and on segment.close()
   * 
   * @param close  Whether to close the buffer after flushing it.  
   * Writes to this segment are no longer possible once the buffer is closed.
   * (the segment can be re-opened of course)
   * @throws IOException
   */
  int flushBufferInternal(boolean close) throws IOException {
    synchronized (flushSync) {
    
    if (!isOnline()) {
      return 0;
    }
    
    // 1. write out snapshot
    TripleFileReader file = writeSnapshot(close);
    
    if (null == file) {
      // no file was written.
      return buffer.getLastDumpBytes();
    }
    
    // 2. open newly created triple file (not anymore.)
    //file.open();
    
    synchronized (this) { // prevent compactions
    
      // 3. add new file, prepare segment info file write
      segmentMetaData.addFile(file);
      segmentMetaData.prepareWrite();
    
      // 4. release buffer snapshot, update files
      synchronized (scanners) { // prevent scanner creation between add() and invalidate()
    
        // update files: this creates a copy of the files list
        files = segmentMetaData.getFiles();
    
        buffer.clearSnapshot();
      
        if (null != flusher) {
          // check to see whether we need to clear the flushRequested flag
          // or otherwise requesting the next flush
          synchronized (buffer) {
            flushRequested = buffer.shouldFlush();
            if (flushRequested) {
              // wow... writes are flying in like crazy
              flusher.request(getId());
            }
          }
        }
    
        // 5. invalidate scanners
        for (SegmentScanner scanner : scanners) {
          scanner.invalidate();
        }
      
      } //synchronized (scanners)
    
      // 6. commit info file write
      segmentMetaData.commitWrite();
      
      // 7. request compaction
      if (null != compactor) {
        if (!compactionRequested && conf.getMinCompaction() <= files.size()) {
          compactor.request(getId());
          compactionRequested = true;
        }
      }
      
    } //synchronized (this)
    
    return buffer.getLastDumpBytes();
    
    } //synchronized (flushSync)
  }
  
  
  // assumes synchronization
  private TripleFileReader writeSnapshot(boolean close) throws IOException {
    
    TripleFileWriter writer = segmentMetaData.createTripleFile();
    
    long writeTime = System.currentTimeMillis();
    
    boolean dumped = buffer.dumpSnapshot(writer, close);
    TripleFileReader reader = writer.closeAndOpen();
    
    writeTime = System.currentTimeMillis() - writeTime;
    
    // no triples dumped
    if (!dumped) {
      LOG.info("Nothing to flush (sgement buffer was empty)");
      return null;
    }
    
    // no triples written (dumped triples had multiplicity of 0)
    if (null == reader) {
      buffer.clearSnapshot();
      LOG.info(String.format("Nothing to write, dumped %.2fMB", 
          LogFormatUtil.MB(buffer.getLastDumpBytes())));
      return null;
    }
    
    TripleFileInfo triplefileInfo = writer.getFileInfo();
    LOG.info(String.format("Segment buffer flushed%s: %.2f MB; free: %.2f MB. " +
        " File #%d written: %.2fMB (%.2fMB), %.2fMB/s (%.2fMB/s).", 
        close ? " AND CLOSED" : "",
        LogFormatUtil.MB(buffer.getLastDumpBytes()),
        LogFormatUtil.MB(buffer.getFree()),
        files.size()+1,
        LogFormatUtil.MB(triplefileInfo.getSize()),
        LogFormatUtil.MB(triplefileInfo.getUncompressedDataSize()),
        LogFormatUtil.MBperSec(triplefileInfo.getSize(), writeTime),
        LogFormatUtil.MBperSec(triplefileInfo.getUncompressedDataSize(), writeTime)));
    
    return reader;
  }
  
   
  /**
   * <p> Closes this segment.  Also writes out current buffer to disk.
   * 
   */
  @Override
  public void close() throws IOException {
    LOG.info("Closing segment " + this);
    
    if (isOnline()) {
      // flush buffer and take segment offline
      takeOffline();
    }
    
    closed = true;
    
    synchronized (this) { // no compactions, flushes
    
      synchronized (scanners) { // no scanner creations / reseeks
        
        // invalidate scanners
        for (SegmentScanner scanner : scanners) {
          scanner.invalidate();
        }
        // close files
        for (TripleFileReader file : files) {
          file.close();
        }
        
      } // synchronized (scanners)
      
    } // synchronized (this)
    
    if (flushRequested) {
      flusher.removeRequest(getId());
    }
    if (compactionRequested) {
      compactor.removeRequest(getId());
    }
  }
  
  
  /**
   * Try closing this segment without flushing it.  This only 
   * succeeds if the segment is empty.
   * @return  True if the segment was closed, false otherwise.
   */
  public boolean closeIfEmpty() {
    synchronized (this) {
      if (!(files.isEmpty() && buffer.tryClose())) {
        return false;
      }
      closed = true;
      return true;
    }
  }
  
  
  public static class SegmentClosedException extends IOException {
    private static final long serialVersionUID = 2630907055930954035L;

    public SegmentClosedException() {
      super();
    }
    
    public SegmentClosedException(Throwable cause) {
      super(cause);
    } 
  }
  
  
  public boolean isClosed() {
    return closed;
  }
  
  
  public void flushCompactClose() throws IOException {
    
    if (isOnline()) {
      // flush & offline
      takeOffline();
    }
    
    // run major compaction
    compact(true);
    
    // close files etc
    close();
    
  }
  
  
/*  private static double[] compactionBW  = new double[MAX_COMPACT_FILES];
  private static long[]   compactionB   = new long[MAX_COMPACT_FILES];
  private static int[]    compactionCNT = new int[MAX_COMPACT_FILES];
  
  public static String getCompactionStats() {
    double totalBW = 0;
    long totalB = 0;
    int totalCNT = 0;
    StringBuilder str = new StringBuilder();
    str.append("Compaction stats\n");
    
    for (int i=1; i<MAX_COMPACT_FILES; ++i) {
      
      str.append(String.format("%d: %d times, avg. %.2fMB/sec, total %.2fMB\n", 
          i+1, compactionCNT[i], 
          0==compactionCNT[i]?0:compactionBW[i]/compactionCNT[i], 
          0==compactionCNT[i]?0:LogFormatUtil.MB(compactionB[i])));
      totalBW += compactionBW[i];
      totalB += compactionB[i];
      totalCNT += compactionCNT[i];
    }
    str.append(String.format("Totals: %d times, avg. %.2fMB/sec, total %.2fMB\n",
        totalCNT, totalBW/totalCNT, LogFormatUtil.MB(totalB)));
    return str.toString();
  }
  
  public static void resetCompactionStats() {
    compactionBW  = new double[MAX_COMPACT_FILES];
    compactionB   = new long[MAX_COMPACT_FILES];
    compactionCNT = new int[MAX_COMPACT_FILES];
  }*/
  
  
  private long compactBytesWritten = 0;
  
  /**
   * @return Number of (uncompressed) bytes triple data written during compactions.
   */
  public long getCompactBytesWritten() {
    return compactBytesWritten;
  }
  
  
  public synchronized void requestMajorCompaction() {
    compactor.removeRequest(getId());
    compactor.requestUrgent(getId());
    compactionRequested = true;
  }
  
  
  private TripleFileReader compact(List<TripleFileReader> filesToCompact) 
      throws IOException {
    // open scanner for each file to compact
    TripleScanner[] scanners = new TripleScanner[filesToCompact.size()];
    int i = 0;
    for (TripleFileReader file : filesToCompact) {
      scanners[i++] = file.getScanner();
    }
    // create a merged scanner
    TripleScanner merged = new MergedScanner(scanners);
    
    // write out triples
    TripleFileWriter writer = segmentMetaData.createTripleFile();
    
    long writeTime = System.currentTimeMillis();
    while (merged.next()) {
      writer.appendTriple(merged.pop());
    }
    TripleFileReader reader = writer.closeAndOpen();
    writeTime = System.currentTimeMillis() - writeTime;
    merged.close();
    
    // no triples written (triple files canceled each other out)
    if (null == reader) {
      LOG.info("segment compaction: no triples left.");
      return null;
    }
    
    TripleFileInfo fileInfo = writer.getFileInfo();
    LOG.info(String.format("%d files compacted: %.2fMB (%.2fMB), %.2fMB/s (%.2fMB/s).", 
        filesToCompact.size(),
        LogFormatUtil.MB(fileInfo.getSize()),
        LogFormatUtil.MB(fileInfo.getUncompressedSize()),
        LogFormatUtil.MBperSec(fileInfo.getSize(), writeTime),
        LogFormatUtil.MBperSec(fileInfo.getUncompressedSize(), writeTime)));
    
//    compactionBW[filesToCompact.size()-1] 
//                   += LogFormatUtil.MBperSec(fileInfo.getUncompressedLength(), writeTime);
//    compactionB[filesToCompact.size()-1] += fileInfo.getUncompressedLength();
//    compactionCNT[filesToCompact.size()-1] ++;
    compactBytesWritten += fileInfo.getUncompressedDataSize();
    
    return reader;
  }
  
  
  public void compact(boolean majorCompaction) throws IOException {
    
    if (isClosed()) {
      LOG.info("Segment is closed.  Cancelling compaction.");
      return;
    }
    
    synchronized (compactionSync) {
    
    List<TripleFileReader> filesToCompact = getFilesToCompact(majorCompaction);
    
    if (null == filesToCompact) {
      compactionRequested = false;
      return;
    }
    
    TripleFileReader cfile = compact(filesToCompact);
      
    synchronized (this) { // prevent flush/close
           
      // remove all compacted files
      segmentMetaData.removeAllFiles(filesToCompact);
      
      // add new file
      if (null != cfile) {
        segmentMetaData.addFile(cfile);
      }
      
      // prepare for writing new segment info
      segmentMetaData.prepareWrite();
      
      synchronized (scanners) { // prevent new scanners
        
        // update files (copy)
        files = segmentMetaData.getFiles();
      
        // invalidate scanners
        for (SegmentScanner scanner : scanners) {
          scanner.invalidate();
        }
        
      } //synchronized (scanners)
      
      // commit segment info file write
      segmentMetaData.commitWrite();
      
      
      // check: do we need another compaction?
      if (conf.getMinCompaction() <= files.size()) {
        // possibly... schedule another one
        compactor.request(getId());
        compactionRequested = true;
      } else {
        // nope, reset request flag
        compactionRequested = false;
      }

    } //synchronized (this)
    
    // close & delete old files
    for (TripleFileReader file : filesToCompact) {
      file.close();
      segmentMetaData.deleteFile(file);
    }
    
    } //synchronized (compactionSync)
  }
  
   
  /**
   * <p> This method implements a heuristic for selecting files to compact.
   * 
   * <p> The heuristic is close to the one used buy HBase, see
   * https://issues.apache.org/jira/browse/HBASE-2462
   * 
   * @return  A list of files to compact.
   */
  private List<TripleFileReader> getFilesToCompact(boolean majorCompaction) {
    
    List<TripleFileReader> candidates;
    synchronized (this) {
      candidates = new ArrayList<TripleFileReader>(files);
    }
    
    if (2 > candidates.size()) {
      LOG.info("Only one file, cancelling compation.");
      return null;
    }
    
    if (majorCompaction) {
      LOG.info(String.format("Major compaction (%d files).", candidates.size()));
      return candidates;
    }
      
    // sort files by size
    Collections.sort(candidates, 
        new Comparator<TripleFileReader>() {
          @Override
          public int compare(TripleFileReader file1, TripleFileReader file2) {
            return (int) (file1.getFileSize() - file2.getFileSize());
          }
    });
    
    List<TripleFileReader> filesToCompact = new ArrayList<TripleFileReader>();
    long sum = 0;
    
    for (TripleFileReader file : candidates) {
      if (conf.getMinCompactionSize() > file.getFileSize()) {
        // small file, add it
        filesToCompact.add(file);
        sum += file.getFileSize();
       } else if (conf.getMaxCompaction() > filesToCompact.size()
          && file.getFileSize() < sum * conf.getCompactionRatio()) {
        // large file, add it if size(file) < sum(small files) * ratio
        filesToCompact.add(file);
        sum += file.getFileSize();
        break;
      }
      if (conf.getMaxCompaction() == filesToCompact.size()) {
        // max number of files to compact reached
        break;
      }
    }
    
    if (conf.getMinCompaction() > filesToCompact.size()) {
      // see if we need to get rid of any half-files 
      if (conf.getSplitThreshold() < getSize() && !canBeSplit()) {
        // yes, add all half files (otherwise deadlock is possible)
        for (TripleFileReader file : candidates) {
          if (file instanceof HalfFileReader && !filesToCompact.contains(file)) {
            filesToCompact.add(file);
          }
        }
        LOG.info("Segment needs split but half files are present --> all half files added" +
        		" to compaction");
      } else {
        // too few files to compact, cancel compaction
        LOG.info("Too few files for minor compaction, compaction cancelled.");
        return null;
      }
    }
    
    // min is 2 files
    if (2 > filesToCompact.size()) {
      LOG.info("Need at least 2 files for compaction, compaction cancelled.");
      return null;
    }
    
    LOG.info(String.format("Selected %d files for minor compaction, total size = %.2f MB.",
        filesToCompact.size(), LogFormatUtil.MB(sum)));
    
    return filesToCompact;    
  }
  
  
  private TripleScanner getScannerInternal(Triple pattern) throws IOException {
        
    synchronized (scanners) {
      
      if (isClosed()) {
        return null;
      }
      
      List<TripleScanner> scanners = new ArrayList<TripleScanner>();
      
      // get file scanners    
      for (TripleFileReader reader : files) {
        TripleScanner scanner = null == pattern
            ? reader.getScanner()
            : reader.getScanner(pattern);
      
        if (null != scanner) {
          scanners.add(scanner);
        }
      }
      
      if (isOnline()) {
        TripleScanner bscanner = null == pattern 
          ? buffer.getScanner() : buffer.getScanner(pattern);
        if (null != bscanner) {
          scanners.add(bscanner);
        }
      }
      
      if (scanners.isEmpty()) {
        return null;
      } else if (1 == scanners.size()) {
        return scanners.get(0);
      }
      return new MergedScanner(scanners.toArray(new TripleScanner[scanners.size()])); 

    }
  }
  
  
  private TripleScanner getScannerAtInternal(Triple pattern) throws IOException {
    
    synchronized (scanners) {
      
      if (isClosed()) {
        return null;
      }
      
      List<TripleScanner> scanners = new ArrayList<TripleScanner>();
      
      // get file scanners    
      for (TripleFileReader reader : files) {
        TripleScanner scanner = null == pattern
            ? reader.getScanner()
            : reader.getScannerAt(pattern);
      
        if (null != scanner) {
          scanners.add(scanner);
        }
      }
      
      if (isOnline()) {
        TripleScanner bscanner = null == pattern 
          ? buffer.getScanner() : buffer.getScannerAt(pattern);
        if (null != bscanner) {
          scanners.add(bscanner);
        }
      }
      
      if (scanners.isEmpty()) {
        return null;
      } else if (1 == scanners.size()) {
        return scanners.get(0);
      }
      return new MergedScanner(scanners.toArray(new TripleScanner[scanners.size()])); 

    }
  }
  
  
  private TripleScanner getScannerAfterInternal(Triple pattern) throws IOException {
    
    synchronized (scanners) {
            
      if (isClosed()) {
        return null;
      }
      
      List<TripleScanner> scanners = new ArrayList<TripleScanner>();
      
      // get file scanners 
      for (TripleFileReader reader : files) {
        TripleScanner scanner = null == pattern
              ? reader.getScanner()
              : reader.getScannerAfter(pattern);
    
        if (null != scanner) {
          scanners.add(scanner);
        }
      }
      
      if (isOnline()) {
        TripleScanner bscanner = null == pattern 
          ? buffer.getScanner() : buffer.getScannerAfter(pattern);
        if (null != bscanner) {
          scanners.add(bscanner);
        }
      }
      
      if (scanners.isEmpty()) {
        return null;
      } else if (1 == scanners.size()) {
        return scanners.get(0);
      }
      return new MergedScanner(scanners.toArray(new TripleScanner[scanners.size()])); 
    }
  }
  
  
  /**
   * Segment scanner implementation.  Delegates to an internal scanner
   * that is re-opened and re-seeked when necessary.
   * 
   * @author daniel.hefenbrock
   *
   */
  class SegmentScanner extends TripleScanner {

    /**
     * Scanner to delegate to.
     */
    private TripleScanner scanner;
    
    private Triple prevTriple = null;
    
    /**
     * This flag is used to indicate that scanners need be re-opened and
     * re-seeked right after prevTriple.
     */
    private volatile boolean reseek = false;    
    
    
    SegmentScanner(TripleScanner scanner) {
      this.scanner = scanner;
    }
    
    public void invalidate() {
      reseek = true;
    }
    
    /**
     * @return False if the segment is closed.
     */
    private boolean reseek() throws IOException {
      reseek = false;
      scanner = Segment.this.getScannerAfterInternal(prevTriple);
      
      if (null == scanner) {
        return !Segment.this.isClosed();
      }
      return true;
    }
    
    
    @Override
    public COLLATION getOrder() {
      return Segment.this.getOrder();
    }
    

    @Override
    protected Triple nextInternal() throws IOException {
      
      if (null == scanner) {
        // the internal scanner is empty
        return null;
      }
      
      do {
      
        if (reseek) {
          // crap.  scanners are no longer valid          
          if (!reseek() || null == scanner) {
            // the internal scanner is empty
            return null;
          }
        }
      
        try {
          if (!scanner.next()) {
            // we're done
            return null;
          }
        } catch (TripleFileReader.FileClosedException ex) {
          // file was closed (for example, because a compaction happened)
          // we need to re-open and re-seek
          reseek = true;
          continue;
        }
      
        prevTriple = scanner.pop(); 
        
      } while (null == prevTriple);
      
      return prevTriple;
    }

    @Override
    public void close() throws IOException {
      if (null != scanner) {
        scanner.close();
      }
      Segment.this.unregisterScanner(this);
      scanner = null;
      prevTriple = null;
    }
    
    
    @Override
    public String toString() {
      return "SegmentScanner(" + scanner.toString() + ")";
    }
    
  }
  
  
  
  public SegmentSize getSegmentSize() {
    synchronized (this) {
      long fileSize = 0;
      long uncompressedFileSize = 0;
      long dataSize = 0;
      int indexSize = 0;
      long nTriples = 0;
      for (TripleFileReader file : files) {
        fileSize += file.getFileSize();
        uncompressedFileSize += file.getUncompressedFileSize();
        dataSize += file.getUncompressedDataSize();
        nTriples += file.getNumberOfTriples();
        indexSize += file.getIndexSize();
      }
      return new SegmentSize(fileSize, uncompressedFileSize,
          files.size(), dataSize, nTriples, buffer.getSize(), buffer.resetBytesAdded(),
          indexSize);
    }
  }
  
  public static class SegmentSize implements Writable {
    public SegmentSize() {}
    
    private SegmentSize(long fileSize, long uncompressedFileSize, long numberOfFiles, 
        long dataSize, long nTriples, int bufferSize, int bytesAdded, int indexSize) {
      this.fileSize = fileSize;
      this.uncompressedFileSize = uncompressedFileSize;
      this.numberOfFiles = numberOfFiles;
      this.dataSize = dataSize;
      this.nTriples = nTriples;
      this.bufferSize = bufferSize;
      this.bytesAdded = bytesAdded;
      this.indexSize = indexSize;
    }
    
    private long fileSize;
    private long uncompressedFileSize;
    private long numberOfFiles;
    private long dataSize;
    private long nTriples;
    private int bufferSize;
    private int bytesAdded;
    private int indexSize;
    
    public long getFileSize() {
      return fileSize;
    }
    
    public long getUncompressedFileSize() {
      return uncompressedFileSize;
    }
    
    public long getNumberOfFiles() {
      return numberOfFiles;
    }
    
    public long getDataSize() {
      return dataSize;
    }
    
    public long getNumberOfTriples() {
      return nTriples;
    }
    
    public int getBufferSize() {
      return bufferSize;
    }
    
    public int getBytesAdded() {
      return bytesAdded;
    }
    
    public int getIndexSize() {
      return indexSize;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      fileSize = in.readLong();
      uncompressedFileSize = in.readLong();
      numberOfFiles = in.readLong();
      dataSize = in.readLong();
      nTriples = in.readLong();
      bufferSize = in.readInt();
      bytesAdded = in.readInt();
      indexSize = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(fileSize);
      out.writeLong(uncompressedFileSize);
      out.writeLong(numberOfFiles);
      out.writeLong(dataSize);
      out.writeLong(nTriples);
      out.writeInt(bufferSize);
      out.writeInt(bytesAdded);
      out.writeInt(indexSize);
    }
  }
  
  
  public TripleFileInfo[] getFileInfo() {
    synchronized (this) {
      TripleFileInfo[] info = new TripleFileInfo[files.size()];
      int i = 0;
      for (TripleFileReader file : files) {
        info[i++] = file.getFileInfo();
      }
      return info;
    }
  }


}
