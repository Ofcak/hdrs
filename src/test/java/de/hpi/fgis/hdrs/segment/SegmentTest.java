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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.node.SegmentCompactorThread;
import de.hpi.fgis.hdrs.node.SegmentFlusherThread;
import de.hpi.fgis.hdrs.node.SegmentServer;
import de.hpi.fgis.hdrs.node.SegmentCompactorThread.CompactionRequest;
import de.hpi.fgis.hdrs.tio.TripleGenerator;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class SegmentTest {
  
  static final int N = 1000;
  
  
  @Test
  public void testConcurrentWriteFlushCompactScan() throws IOException, InterruptedException {
    
    final Segment seg = Segment.createTmpSegment(Triple.COLLATION.SPO, 10 * 1024, 5 * 1024);
        
    WriteCountThread writer1 = new WriteCountThread(seg, N);
    WriteCountThread writer2 = new WriteCountThread(seg, N);
    
    ScanThread scanthread = new ScanThread(seg);
    
    SegmentServer server = new SegmentServer() {
      @Override
      public int compactSegment(Set<CompactionRequest> requests) throws IOException {
        seg.compact(false);
        return 1;
      }
      @Override
      public boolean flushSegment(long segmentId) throws IOException {
        seg.flushBuffer();
        return true;
      }
      @Override
      public boolean splitSegment(long segmentId) throws IOException {
        return false;
      }
      @Override
      public void stop(Throwable t) {
        t.printStackTrace();
      }
    };
    
    SegmentFlusherThread flusher = new SegmentFlusherThread(server);
    seg.setSegmentFlusher(flusher);
    
    SegmentCompactorThread compactor = new SegmentCompactorThread(server);
    seg.setSegmentCompactor(compactor);
    
    writer1.start();
    writer2.start();
    flusher.start();
    compactor.start();
    
    scanthread.start();
    scanthread.join();
    
    //System.out.println(seg.segmentInfo());
    
    //seg.close();
    seg.flushCompactClose();
    
    
    flusher.quit();
    flusher.join();
    
    compactor.quit();
    compactor.join();
    
    if (null != scanthread.ioe) {
      throw scanthread.ioe;
    }
     
    if (null != writer1.ioe) {
      throw writer1.ioe;
    }
    
    if (null != writer2.ioe) {
      throw writer2.ioe;
    }
  
    writer1.join();
    writer2.join();
    
    
    Segment.deleteTmpSegment(seg);
  }
  
   
  public class ScanThread extends Thread {
    Segment segment;
    IOException ioe = null;
    
    public ScanThread(Segment seg) {
      segment = seg;
    }
    
    @Override
    public void run()  {
      Triple.Comparator cmp = segment.getOrder().comparator();
      int prevcnt = 0;
      while (true) {
        //System.out.println("SCAN");
        try {
          TripleScanner scanner = segment.getScanner();
          if (null == scanner) {
            continue;
          }
          int cnt = 0;
          Triple prev = null;
          while (scanner.next()) {
            Triple t = scanner.pop();
            assertEquals(1, t.getMultiplicity());
            cnt++;
            //System.out.println(t);
            if (null != prev) {
              assertTrue(0 > cmp.compare(prev, t));
            }
            prev = t;
          }
          //System.out.println(cnt + ": " + scanner);
          //System.out.println(cnt);
          assertTrue(cnt >= prevcnt);
          prevcnt = cnt;
          
          if (2*N == cnt) {
            //System.out.println("DONE");
            assertTrue(!scanner.next());
            break;
          }
        } catch (IOException ex) {
          ex.printStackTrace();
          ioe = ex;
          break;
        }
      }
    }
    
  }
  
  
  public class WriteCountThread extends Thread {
    private final Segment segment;
    private final int amount;
    IOException ioe = null;
    
    public WriteCountThread(Segment seg, int amount) {
      segment = seg;
      this.amount = amount;
    }
    
    @Override
    public void run() {
      TripleGenerator gen = new TripleGenerator();
      
      for (int i=0; i<amount; ++i) {
        Triple t = gen.generate(this.getName()+i);
        try {
          assertTrue(segment.add(t));
        } catch (IOException ex) {
          ex.printStackTrace();
          ioe = ex;
          break;
        }
      }
      //System.out.println(this + "exit");
    }
    
  }
  
  
  
  
  @Test
  public void testConcurrentWriteFlush() throws IOException, InterruptedException {
    
    final Segment seg = Segment.createTmpSegment(Triple.COLLATION.SPO, 10 * 1024, 5 * 1024);
    
    SegmentServer server = new SegmentServer() {
      @Override
      public int compactSegment(Set<CompactionRequest> requests) throws IOException {
        seg.compact(false);
        return 1;
      }
      @Override
      public boolean flushSegment(long segmentId) throws IOException {
        seg.flushBuffer();
        return true;
      }
      @Override
      public boolean splitSegment(long segmentId) throws IOException {
        return false;
      }
      @Override
      public void stop(Throwable t) {
        t.printStackTrace();
      }
    };
    
    WriterThread writer1 = new WriterThread(seg);
    WriterThread writer2 = new WriterThread(seg);
    
    SegmentFlusherThread flusher = new SegmentFlusherThread(server);
    seg.setSegmentFlusher(flusher);
    
    writer1.start();
    writer2.start();
    flusher.start();
    
    Thread.sleep(50);
    
    seg.close();
    
    flusher.quit();
    flusher.join();
    
    writer1.interrupt();
    writer2.interrupt();
     
    if (null != writer1.ioe) {
      throw new IOException(writer1.ioe);
    }
    
    if (null != writer2.ioe) {
      throw new IOException(writer2.ioe);
    }
  
    writer1.join();
    writer2.join();
    
    Segment.deleteTmpSegment(seg);
  }
  
  
  public class WriterThread extends Thread {
    private final Segment segment;
    IOException ioe = null;
    
    public WriterThread(Segment seg) {
      segment = seg;
    }
    
    @Override
    public void run() {
      TripleGenerator gen = new TripleGenerator();
      while (segment.isOnline()) {
        try {
          if (!segment.add(gen.generate())) {
            break;
          }
        } catch (IOException ex) {
          ioe = ex;
          break;
        }
      }
      //System.out.println(this + "exit");
    }
    
  }
  
  
  
  @Test
  public void testRepeatedWrite() throws IOException, InterruptedException {
    
    final Segment seg = Segment.createTmpSegment(Triple.COLLATION.SPO, 10 * 1024, 5 * 1024);
        
    SegmentServer server = new SegmentServer() {
      @Override
      public int compactSegment(Set<CompactionRequest> requests) throws IOException {
        seg.compact(false);
        return 1;
      }
      @Override
      public boolean flushSegment(long segmentId) throws IOException {
        seg.flushBuffer();
        return true;
      }
      @Override
      public boolean splitSegment(long segmentId) throws IOException {
        return false;
      }
      @Override
      public void stop(Throwable t) {
        t.printStackTrace();
      }
    };
    
    SegmentFlusherThread flusher = new SegmentFlusherThread(server);
    seg.setSegmentFlusher(flusher);
    
    SegmentCompactorThread compactor = new SegmentCompactorThread(server);
    seg.setSegmentCompactor(compactor);
    
    flusher.start();
    compactor.start();
    
    Triple t = Triple.newTriple("Subject", "Subject", "Subject");
    for (int i=0; i<1000000; ++i) {
      assertTrue(seg.add(t.clone())); // clone is important!!
    }
    
    //seg.close();
    seg.flushCompactClose();
    
    
    flusher.quit();
    flusher.join();
    
    compactor.quit();
    compactor.join();
    
    Segment.deleteTmpSegment(seg);
  }
  
  
  
  @Test
  public void testEmptyWrite() throws IOException, InterruptedException {
    
    final Segment seg = Segment.createTmpSegment(Triple.COLLATION.SPO, 10 * 1024, 5 * 1024);
        
    SegmentServer server = new SegmentServer() {
      @Override
      public int compactSegment(Set<CompactionRequest> requests) throws IOException {
        seg.compact(false);
        return 1;
      }
      @Override
      public boolean flushSegment(long segmentId) throws IOException {
        seg.flushBuffer();
        return true;
      }
      @Override
      public boolean splitSegment(long segmentId) throws IOException {
        return false;
      }
      @Override
      public void stop(Throwable t) {
        t.printStackTrace();
      }
    };
    
    SegmentFlusherThread flusher = new SegmentFlusherThread(server);
    seg.setSegmentFlusher(flusher);
    
    SegmentCompactorThread compactor = new SegmentCompactorThread(server);
    seg.setSegmentCompactor(compactor);
    
    flusher.start();
    compactor.start();
    
    //seg.close();
    seg.flushCompactClose();
    
    
    flusher.quit();
    flusher.join();
    
    compactor.quit();
    compactor.join();
    
    Segment.deleteTmpSegment(seg);
  }
  
  
  
  @Test
  public void testZeroWrite() throws IOException, InterruptedException {
    
    final Segment seg = Segment.createTmpSegment(Triple.COLLATION.SPO, 10 * 1024, 5 * 1024);
        
    SegmentServer server = new SegmentServer() {
      @Override
      public int compactSegment(Set<CompactionRequest> requests) throws IOException {
        seg.compact(false);
        return 1;
      }
      @Override
      public boolean flushSegment(long segmentId) throws IOException {
        seg.flushBuffer();
        return true;
      }
      @Override
      public boolean splitSegment(long segmentId) throws IOException {
        return false;
      }
      @Override
      public void stop(Throwable t) {
        t.printStackTrace();
      }
    };
    
    SegmentFlusherThread flusher = new SegmentFlusherThread(server);
    seg.setSegmentFlusher(flusher);
    
    SegmentCompactorThread compactor = new SegmentCompactorThread(server);
    seg.setSegmentCompactor(compactor);
    
    flusher.start();
    compactor.start();
    
    Triple t = Triple.newTriple("Subject", "Subject", "Subject", 0);
    assertTrue(seg.add(t.clone())); // clone is important!!
    
    //seg.close();
    seg.flushCompactClose();
    
    
    flusher.quit();
    flusher.join();
    
    compactor.quit();
    compactor.join();
    
    Segment.deleteTmpSegment(seg);
  }
  
  
  
  @Test
  public void testCreateDelete() throws IOException {
    Segment seg = Segment.createTmpSegment(Triple.COLLATION.SPO, 10 * 1024, 5 * 1024);
    assertTrue(seg.add(Triple.newTriple("A", "B", "C")));
    seg.close();
    Segment.deleteTmpSegment(seg);
  }
  
  
  
}
