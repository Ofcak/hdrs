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

package de.hpi.fgis.hdrs.tio;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;


public class TripleScannerTest {

  
  @Test
  public void testSeek() throws IOException {
    
    Triple[] t = new Triple[8];
    t[0] = Triple.newTriple("A", "A", "A");
    t[1] = Triple.newTriple("A", "A", "A");
    t[2] = Triple.newTriple("B", "A", "A");
    t[3] = Triple.newTriple("C", "A", "A");
    t[4] = Triple.newTriple("C", "A", "A");
    t[5] = Triple.newTriple("D", "A", "A");
    t[6] = Triple.newTriple("D", "A", "A");
    t[7] = Triple.newTriple("D", "A", "A");
    
    TripleScanner s = new MergedScannerTest.IteratorScanner(Arrays.asList(t));
    
    Triple.Comparator cmp = Triple.COLLATION.SPO.comparator();
    
    Triple p;
    
    p = Triple.newTriple("A", "A", "A");
    assertTrue(s.seek(p));
    assertEquals(0, cmp.compare(s.peek(), p));
    
    p = Triple.newTriple("A", "B", "A");
    assertFalse(s.seek(p));
    assertEquals(0, cmp.compare(s.peek(), t[2]));
    
    p = Triple.newPattern("B", null, null);
    assertTrue(s.seek(p));
    assertEquals(0, cmp.compare(s.peek(), t[2]));
    
    p = Triple.newPattern("B", "B", null);
    assertFalse(s.seek(p));
    assertEquals(0, cmp.compare(s.peek(), t[3]));
    
    p = Triple.newPattern("D", "D", null);
    assertFalse(s.seek(p));
    assertFalse(s.next());
    
  }
  
}
