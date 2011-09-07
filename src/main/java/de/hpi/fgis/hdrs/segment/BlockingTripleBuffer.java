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

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hpi.fgis.hdrs.LogFormatUtil;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.tio.MergedScanner;
import de.hpi.fgis.hdrs.tio.OrderedTripleSink;
import de.hpi.fgis.hdrs.tio.SeekableTripleSource;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.tio.TripleSink;

/**
 * <p> A bounded triple buffer.  This buffer is designed to block writers
 * once its capacity is exceeded, this way preventing us from running 
 * out of heap space.
 * 
 * <p> The buffer can be flushed to disk in order to free up space.
 * 
 * @author daniel.hefenbrock
 *
 */
class BlockingTripleBuffer implements TripleSink, SeekableTripleSource {
    
  private ConcurrentSortedTripleList tripleList;
  private ConcurrentSortedTripleList snapshot = null;
  
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  private final int capacity;
  private final int flushThreshold;
  private final Semaphore freeBytes;
  
  private final AtomicInteger bytesAdded = new AtomicInteger();
  
  private int lastDumpBytes = 0;
  
  /**
   * In closed state, the buffer can neither be read nor written.
   */
  private volatile boolean closed = true;
  
  
  public BlockingTripleBuffer(Triple.COLLATION order, int capacity, int flushThreshold) {
    assert(flushThreshold <= capacity);
    tripleList = new ConcurrentSortedTripleList(order);
    this.capacity = capacity;
    this.flushThreshold = flushThreshold;
    freeBytes = new Semaphore(capacity);
  }
  
  /**
   * Enable reads/writes for this buffer.
   */
  public void open() {
    closed = false;
  }
  
  public int getCapacity() {
    return capacity;
  }
  
  public int getFree() {
    return freeBytes.availablePermits();
  }
  
  public int getSize() {
    return capacity - freeBytes.availablePermits();
  }
  
  int getLastDumpBytes() {
    return lastDumpBytes;
  }
  
  public int getFlushThreshold() {
    return flushThreshold;
  }
  
  public boolean shouldFlush() {
    return getSize() >= flushThreshold;
  }
  
  public boolean isClosed() {
    return closed;
  }
  
  public boolean hasThreadsWaiting() {
    return freeBytes.hasQueuedThreads();
  }
  
  
  public int batchAdd(int nBytes, TripleScanner triples) throws IOException {
    
    // check closed
    if (isClosed()) {
      return 0;
    }
    
    try {
      // get a permit for writing nBytes bytes
      freeBytes.acquire(nBytes);
    } catch (InterruptedException ex) {
      // someone woke up this thread
      return 0;
    }
    
    // get a read lock:  a snapshot cannot be started while we do this
    lock.readLock().lock();
    
    // check closed again
    if (isClosed()) {
      lock.readLock().unlock();
      freeBytes.release(nBytes);
      return 0;
    }
    
    int nBytesAdded = 0;
    
    while (triples.next()) {
      Triple triple = triples.peek();
      nBytesAdded += triple.estimateSize();
      if (nBytesAdded > nBytes) {
        nBytesAdded -= triple.estimateSize();
        break;
      }
      // add triple
      if (!tripleList.add(triple)) {
        // awesome!  triple was already present, get back our bytes!
        // UPDATE:   This actually introduces the possibility of a memory
        //           leak if a large number of equal tripels are written.
        nBytesAdded -= triple.estimateSize();
      }
      triples.pop();
    }
    lock.readLock().unlock();
    
    nBytes -= nBytesAdded;
    if (0 < nBytes) {
      // less was written than requested, return unused bytes
      freeBytes.release(nBytes);
    }
    
    // update counter
    bytesAdded.addAndGet(nBytesAdded);
    
    return nBytesAdded;
  }
  
  
  @Override
  public boolean add(Triple triple) {
    
    // check closed
    if (isClosed()) {
      return false;
    }
    
    int nBytes = triple.estimateSize();
    
    try {
      // get a permit for writing nBytes bytes
      freeBytes.acquire(nBytes);
    } catch (InterruptedException ex) {
      // someone woke up this thread
      return false;
    }
    
    // get a read lock:  a snapshot cannot be started while we do this
    lock.readLock().lock();
    
    // check closed again
    if (isClosed()) {
      lock.readLock().unlock();
      freeBytes.release(nBytes);
      return false;
    }
    
    // add triple
    if (!tripleList.add(triple)) {
      // awesome!  triple was already present, get back our bytes!
      // UPDATE:   This actually introduces the possibility of a memory
      //           leak if a large number of equal tripels are written.
      freeBytes.release(nBytes);
    }
    
    lock.readLock().unlock();
    
    bytesAdded.addAndGet(nBytes);
    
    return true;
  }
  
  
  int resetBytesAdded() {
    return bytesAdded.getAndSet(0);
  }
  
  
  /**
   * Take snapshot, write it to provided triple sink.  This will throw an IOE
   * in case this buffer is already closed.
   * @param out  Triple sink to write snapshot to.
   * @param close  If true, this buffer will be closed after taking the snapshot.
   * @throws IOException  In case buffer is already closed.
   * @return True, if any triples were dumped.
   */
  public boolean dumpSnapshot(OrderedTripleSink out, boolean close) throws IOException {  
     
    if (null != snapshot) {
      throw new IllegalStateException("Cannot dump buffer snapshot before " +
      		"previous snapshot has been cleared.");
    }
    if (out.getOrder() != tripleList.getOrder()) {
      throw new IllegalStateException("Triple sink has wrong order");
    }
    
    lock.writeLock().lock();
    if (isClosed()) { // check again...
      lock.writeLock().unlock();
      throw new IOException("Buffer is closed.");
    }
    closed = close;
    if (tripleList.isEmpty()) {
      lock.writeLock().unlock();
      lastDumpBytes = 0;
      return false;
    }
    snapshot = tripleList;
    tripleList = new ConcurrentSortedTripleList(tripleList.getOrder());
    lock.writeLock().unlock();
    
    int nBytesWritten = 0;
    
    // write out snapshot
    TripleScanner scanner = snapshot.getRawScanner();
    while (scanner.next()) {
      Triple triple = scanner.pop();
      int multiplicity = triple.getMultiplicity();
      // don't write triples that do not exist.
      // still, we  need to count the bytes to correctly free buffer space
      if (0 != multiplicity) {
        out.add(triple);
      }
      nBytesWritten += triple.estimateSize();
    }
    lastDumpBytes = nBytesWritten; 
    return true;
  }
  
  /**
   * Throw away previously dumped snapshot and release memory occupied
   * by it. 
   */
  public void clearSnapshot() {
    if (null == snapshot) {
      throw new IllegalStateException("Cannot clear snapshot: snapshot does " +
      		"not exist.");
    }
    lock.writeLock().lock();
    snapshot = null;
    lock.writeLock().unlock();
    freeBytes.release(lastDumpBytes);
  }
  
  @Override
  public COLLATION getOrder() {
    return tripleList.getOrder();
  }

  @Override
  public TripleScanner getScanner(Triple pattern) throws IOException {
    TripleScanner scanner = null;
    
    lock.readLock().lock();
    // buffer closed?
    if (isClosed()) {
      lock.readLock().unlock();
      throw new BufferClosedException();
    }
    // snapshot in progress?
    if (null != snapshot) {
      // yes, we need to scan both,  triple list AND snapshot
      scanner = snapshot.getScanner(pattern);
    }
    // get buffer scanner
    TripleScanner s = tripleList.getScanner(pattern);
    if (null != scanner) {
      if (null != s) {
        scanner = new MergedScanner(s, scanner); 
      }
    } else {
      scanner = s;
    }
    lock.readLock().unlock();
    
    if (null == scanner || !scanner.next()) {
      return null;
    }
    return scanner;
  }
  
  
  @Override
  public TripleScanner getScannerAt(Triple pattern) throws IOException {
    TripleScanner scanner = null;
    
    lock.readLock().lock();
    // buffer closed?
    if (isClosed()) {
      lock.readLock().unlock();
      throw new BufferClosedException();
    }
    // snapshot in progress?
    if (null != snapshot) {
      // yes, we need to scan both,  triple list AND snapshot
      scanner = snapshot.getScannerAt(pattern);
    }
    // get buffer scanner
    TripleScanner s = tripleList.getScannerAt(pattern);
    if (null != scanner) {
      if (null != s) {
        scanner = new MergedScanner(s, scanner); 
      }
    } else {
      scanner = s;
    }
    lock.readLock().unlock();
    
    if (null == scanner || !scanner.next()) {
      return null;
    }
    return scanner;
  }
  
  
  @Override
  public TripleScanner getScannerAfter(Triple pattern) throws IOException {
  TripleScanner scanner = null;
    
    lock.readLock().lock();
    // buffer closed?
    if (isClosed()) {
      lock.readLock().unlock();
      throw new BufferClosedException();
    }
    // snapshot in progress?
    if (null != snapshot) {
      // yes, we need to scan both,  triple list AND snapshot
      scanner = snapshot.getScannerAfter(pattern);
    }
    // get buffer scanner
    TripleScanner s = tripleList.getScannerAfter(pattern);
    if (null != scanner) {
      if (null != s) {
        scanner = new MergedScanner(s, scanner); 
      }
    } else {
      scanner = s;
    }
    lock.readLock().unlock();
    
    if (null == scanner || !scanner.next()) {
      return null;
    }
    return scanner;
  }
  

  @Override
  public TripleScanner getScanner() throws IOException {
    TripleScanner scanner;
    
    lock.readLock().lock();
    // buffer closed?
    if (isClosed()) {
      lock.readLock().unlock();
      throw new BufferClosedException();
    }
    // snapshot in progress?
    if (null != snapshot) {
      // yes, we need to scan both,  triple list AND snapshot
      scanner = new MergedScanner(tripleList.getScanner(), snapshot.getScanner());
    } else {
      // nope, just triple list
      scanner = tripleList.getScanner();
    }
    lock.readLock().unlock();
    
    return scanner;
  }
  
  
  public static class BufferClosedException extends IOException {
    private static final long serialVersionUID = -5762298948319728295L;

    public BufferClosedException() {
      super();
    }
    
  }
  
  
  @Override
  public String toString() {
    return "BlockingTripleBuffer(order = " + getOrder()
        + ", capacity = " + LogFormatUtil.MB(capacity)
        + ", flush threshold = " + LogFormatUtil.MB(flushThreshold)
        + ", free = " + LogFormatUtil.MB(getFree())
        + ")";
  }
  
  
}
