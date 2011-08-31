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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;


public class MergedScannerTest {

  
  public static class IteratorScanner extends TripleScanner {

    private Iterator<Triple> iterator;
    
    public IteratorScanner(Collection<Triple> triples) {
      iterator = triples.iterator();
    }
    
    @Override
    public COLLATION getOrder() {
      return Triple.COLLATION.SPO;
    }

    @Override
    protected Triple nextInternal() throws IOException {
      return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void close() throws IOException {
      iterator = null;
    }
    
  }
  
  
  @Test
  public void testMergedScanner() throws IOException {
    
    Triple[] t = new Triple[8];
    t[0] = Triple.newTriple("A", "A", "A");
    t[1] = Triple.newTriple("A", "A", "A");
    t[2] = Triple.newTriple("B", "A", "A");
    t[3] = Triple.newTriple("C", "A", "A");
    t[4] = Triple.newTriple("C", "A", "A");
    t[5] = Triple.newTriple("D", "A", "A");
    t[6] = Triple.newTriple("D", "A", "A");
    t[7] = Triple.newTriple("D", "A", "A");
    
    Triple[] out = new Triple[4];
    out[0] = Triple.newTriple("A", "A", "A", (short) 2);
    out[1] = Triple.newTriple("B", "A", "A");
    out[2] = Triple.newTriple("C", "A", "A", (short) 2);
    out[3] = Triple.newTriple("D", "A", "A", (short) 3);
    
    List<Triple> l1 = new ArrayList<Triple>();
    Collections.addAll(l1, t[0], t[2], t[4], t[6], t[7]);
    
    List<Triple> l2 = new ArrayList<Triple>();
    Collections.addAll(l2, t[1], t[3], t[5]);
    
    TripleScanner s1 = new IteratorScanner(l1);
    TripleScanner s2 = new IteratorScanner(l2);
    
    MergedScanner m = new MergedScanner(s1, s2);
    
    for (int i=0; i<out.length; ++i) {
      assertTrue(m.next());
      Triple triple = m.pop();
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(triple, out[i]));
      assertEquals(out[i].getMultiplicity(), triple.getMultiplicity());
    }
    
    assertTrue(!m.next());

  }
  
  
  
}
