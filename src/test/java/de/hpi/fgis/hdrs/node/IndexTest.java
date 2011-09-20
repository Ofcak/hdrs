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

package de.hpi.fgis.hdrs.node;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.Set;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.node.Index.SegmentNotFoundException;
import de.hpi.fgis.hdrs.node.Index.SegmentSplit;
import de.hpi.fgis.hdrs.node.SegmentCompactorThread.CompactionRequest;
import de.hpi.fgis.hdrs.routing.SegmentInfo;
import de.hpi.fgis.hdrs.segment.Segment;
import de.hpi.fgis.hdrs.segment.SegmentConfiguration;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.triplefile.TripleFileTest;
import de.hpi.fgis.hdrs.triplefile.TripleFileWriter;


public class IndexTest {

  @Test
  public void testSegmentSplit() throws IOException, SegmentNotFoundException {
    
    SegmentConfiguration conf = new SegmentConfiguration(2*1024, 1024);
    
    Index index = Index.createTmpIndex(Triple.COLLATION.SPO);
    
    SegmentInfo info = index.getSegments().get(0);
    
    Segment seg = index.openSegment(conf, info);
    
    seg.takeOnline();
    seg.add(Triple.newTriple("A", "A", "A"));
    seg.add(Triple.newTriple("B", "B", "B"));
    seg.takeOffline();
    
    SegmentSplit split = index.splitSegment(info, seg);
    
    index.updateSegments(info, split.top, split.bottom);
    
    seg.close();
    
    Segment t = index.openSegment(conf, split.top);
    Segment b = index.openSegment(conf, split.bottom);
    
    t.close();
    b.close();
    
    index.delete();
  }
  
  
  @Test
  public void testSegmentSplit2() 
  throws IOException, SegmentNotFoundException, InterruptedException {
    
    SegmentConfiguration conf = new SegmentConfiguration(128*1024, 124*1024);
    
    // no more than 6000 otherwise this test gets stuck since
    // it doesnt have a compactor thread running
    int numTriples = TripleFileWriter.DEFAULT_BLOCKSIZE / 10;
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
    
    Index index = Index.createTmpIndex(Triple.COLLATION.SPO);
    
    SegmentInfo info = index.getSegments().get(0);
    
    // flush size is chosen so that we have more than one
    // block in the triple file (block size = 64k)
    final Segment seg = index.openSegment(conf, info);
    
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
      @Override
      public void flushMaintenance() {        
      }
    };
    
    SegmentFlusherThread flusher = new SegmentFlusherThread(server);
    
    seg.setSegmentFlusher(flusher);
    flusher.start();
    
    seg.takeOnline();
    
    for (Triple t : triples) {
      seg.add(t);
    }
    
    seg.flushBuffer();
    SegmentSplit split = index.splitSegment(info, seg);
    
    index.updateSegments(info, split.top, split.bottom);
    
    seg.close();
    
    flusher.quit();
    flusher.join();
    
    
    Segment t = index.openSegment(conf, split.top);
    Segment b = index.openSegment(conf, split.bottom);
    
    TripleScanner scanner;
    
    scanner = t.getScanner();
    assertTrue(null != scanner);
    
    Triple prev = null;
    while (scanner.next()) {
      prev = scanner.pop();
    }
    scanner.close();
    
    scanner = b.getScanner();
    assertTrue(null != scanner);
    assertTrue(scanner.next());
    
    Triple pop = scanner.pop();
    assertTrue(0 > Triple.COLLATION.SPO.comparator().compare(prev, pop));
    while (scanner.next()) {
      scanner.pop();
    }
   
    scanner.close();
    
    t.close();
    b.close();
    
    index.delete();
  }
  
  
}
