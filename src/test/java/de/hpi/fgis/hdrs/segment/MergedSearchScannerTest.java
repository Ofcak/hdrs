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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.tio.MergedScanner;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class MergedSearchScannerTest {

  /*
   * test is unused
   * 
   */
  
  @Test
  public void testMergedSearchScanner() throws IOException {
    
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
    
    ConcurrentSortedTripleList search = new ConcurrentSortedTripleList(Triple.COLLATION.SPO);
    
    TripleScanner merged = new MergedScanner(search.getScanner(), list.getScanner());
    
    Triple.Comparator cmp = Triple.COLLATION.SPO.comparator();
    
    assertTrue(merged.next());
    assertEquals(0, cmp.compare(triples[0], merged.pop()));
    assertTrue(merged.next());
    assertEquals(0, cmp.compare(triples[1], merged.pop()));
    
    search.add(Triple.newTriple("A", "B", "B"));
    assertTrue(merged.next());
    //assertEquals(0, cmp.compare(Triple.newTriple("A", "B", "B"), merged.pop()));
    
    assertTrue(merged.next());
    assertEquals(0, cmp.compare(triples[2], merged.pop()));
    
    
    search.add(Triple.newTriple("C", "C", "C"));
    assertTrue(merged.seekAfter(triples[4]));
    assertEquals(0, cmp.compare(triples[5], merged.pop()));
    
    assertTrue(merged.next());
    //assertEquals(0, cmp.compare(Triple.newTriple("C", "C", "C"), merged.pop()));
    
    
    //assertTrue(merged.seek(Triple.newTriple("A", "B", "B")));
    //assertEquals(0, cmp.compare(Triple.newTriple("A", "B", "B"), merged.pop()));
    
    
    search.add(Triple.newTriple("F", "A", "A", (short) -1));
    assertTrue(merged.next());    
    //assertEquals(0, cmp.compare( triples[7], merged.pop()));
    
  }
  
}
