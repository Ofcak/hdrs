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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.triplefile.TripleFileTest;


public class ConcurrentSortedTripleListTest {

  
  @Test
  public void testConcurrentWrites() throws InterruptedException, IOException {
    
    int nThreads = 10;
    int nTriples = 10000;
    
    ConcurrentSortedTripleList list = new ConcurrentSortedTripleList(Triple.COLLATION.SPO);
    
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(nTriples);
    List<Triple> negativeTriples = new ArrayList<Triple>();
    for (Triple t : triples) {
      Triple negativeTriple = Triple.newInstance(
          t.getBuffer(), t.getOffset(), t.getSubjectLength(), t.getPredicateLength(), 
          t.getObjectLength(), (short) -1);
      negativeTriples.add(negativeTriple);
    }
    
    
    Thread[] threads = new Thread[nThreads];
    for (int i=0; i<nThreads; ++i) {
      threads[i] = new Thread(new TestThread(
          list,
          (0 == i % 2) ? triples : negativeTriples
          ));
      threads[i].start();
    }
    
    
    for (int i=0; i<nThreads; ++i) {
      threads[i].join();
    }
    
    TripleScanner scanner = list.getScanner();
    
    //System.out.println();
    //System.out.println("asdasd");
    //while (scanner.next()) {
    //  System.out.println(scanner.pop());
    //}
    
    assertTrue(!scanner.next());
    
  }
  
  
  static class TestThread implements Runnable {

    private final ConcurrentSortedTripleList list;
    private final Collection<Triple> triples;
    
    public TestThread(ConcurrentSortedTripleList l, Collection<Triple> t) {
      list = l;
      triples = t;
    }
    
    @Override
    public void run() {
      for (Triple t : triples) {
        //System.out.println("Thread " + this + ": put " + t);
        list.add(t.clone());
      }
    }
    
  }
  
  
  @Test
  public void testIteratorScanner() throws IOException {
    
    Triple[] triples = new Triple[10];
    triples[0] = Triple.newTriple("A", "A", "B", (short) 1);
    triples[1] = Triple.newTriple("A", "B", "A", (short) 1);
    triples[2] = Triple.newTriple("B", "A", "A", (short) 1);
    triples[3] = Triple.newTriple("B", "A", "B", (short) 1);
    triples[4] = Triple.newTriple("B", "A", "C", (short) 1);
    triples[5] = Triple.newTriple("C", "A", "A", (short) 1);
    triples[6] = Triple.newTriple("F", "A", "A", (short) 1);
    triples[7] = Triple.newTriple("G", "A", "A", (short) 1);
    triples[8] = Triple.newTriple("G", "A", "A", (short) 1);
    triples[9] = Triple.newTriple("H", "A", "A", (short) 1);
    
    ConcurrentSortedTripleList list = new ConcurrentSortedTripleList(Triple.COLLATION.SPO);
    for (int i=0; i<triples.length; ++i) {
      list.add(triples[i]);
    }
    
    Triple.Comparator cmp = Triple.COLLATION.SPO.comparator();
    TripleScanner scanner;
    Triple t, p;
    
    t = Triple.newTriple("A", "A", "B");
    scanner = list.getScanner(t);
    assertTrue(null != scanner && scanner.next());
    assertEquals(0, cmp.compare(t, scanner.pop()));
    
    t = Triple.newTriple("G", "A", "A");
    scanner = list.getScanner(t);
    assertTrue(null != scanner && scanner.next());
    assertEquals(0, cmp.compare(t, scanner.pop()));
    
    // not in list
    t = Triple.newTriple("G", "A", "B");
    scanner = list.getScannerAt(t);
    assertTrue(null == scanner);
    
    // pattern (B, *, *)
    p = Triple.newPattern("B", null, null);
    t = Triple.newTriple("B", "A", "A");
    scanner = list.getScanner(p);
    assertTrue(null != scanner && scanner.next());
    assertEquals(0, cmp.compare(t, scanner.pop()));
    
  }
  
}
