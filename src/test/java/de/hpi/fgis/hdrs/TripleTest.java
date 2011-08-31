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

package de.hpi.fgis.hdrs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TripleTest {

  @Test
  public void testTripleConstruction() {
    byte[] buf = "xxxTestSubjectTestPredicateTestObjectxxx".getBytes();
    Triple t = Triple.newInstance(buf, 3, (short) 11, (short) 13, 10, 7);
    assertTrue(t.getSubject().equals("TestSubject"));
    assertTrue(t.getPredicate().equals("TestPredicate"));
    assertTrue(t.getObject().equals("TestObject"));
    assertTrue(7 == t.getMultiplicity());
    assertTrue(!t.isPattern());
    assertTrue(3 == t.getOffset());
    assertTrue(11+13+10 == t.bufferSize());
    
    t = Triple.newTriple("TestSubject", "TestPredicate", "TestObject", 1);
    assertTrue(t.getSubject().equals("TestSubject"));
    assertTrue(t.getPredicate().equals("TestPredicate"));
    assertTrue(t.getObject().equals("TestObject"));
    assertTrue(1 == t.getMultiplicity());
    assertTrue(!t.isPattern());
    assertTrue(11+13+10 == t.bufferSize());
  }
  
  @Test
  public void testPatternConstruction() {
    byte[] buf = "xxxTestSubjectTestPredicatexxx".getBytes();
    Triple t = Triple.newInstance(buf, 3, (short) 11, (short) 13, -1, 0);
    assertTrue(t.getSubject().equals("TestSubject"));
    assertTrue(t.getPredicate().equals("TestPredicate"));
    assertTrue(null == t.getObject());
    assertTrue(0 == t.getMultiplicity());
    assertTrue(t.isPattern());
    assertTrue(3 == t.getOffset());
    assertTrue(11+13+0 == t.bufferSize());
    
    t = Triple.newPattern("TestSubject", "TestPredicate", null);
    assertTrue(t.getSubject().equals("TestSubject"));
    assertTrue(t.getPredicate().equals("TestPredicate"));
    assertTrue(null == t.getObject());
    assertTrue(0 == t.getMultiplicity());
    assertTrue(t.isPattern());
    assertTrue(11+13+0 == t.bufferSize());
  }
  
  @Test
  public void testComparator() {
    Triple[] t = new Triple[10];
    t[0] = Triple.newTriple("A", "A", "A");
    t[1] = Triple.newTriple("A", "A", "A");
    t[2] = Triple.newTriple("A", "A", "B");
    
    t[3] = Triple.newPattern("A", "A", null);
    t[4] = Triple.newPattern("B", null, null);
    t[5] = Triple.newPattern("B", null, null);
    t[6] = Triple.newPattern("B", "X", null);
    
    Triple.Comparator cmp = Triple.COLLATION.SPO.comparator();
    
    assertTrue(0 == cmp.compare(t[0], t[1]));
    assertTrue(-1 == cmp.compare(t[1], t[2]));
    assertTrue(1 == cmp.compare(t[2], t[1]));
    
    assertEquals(-1, cmp.compare(t[3], t[0]));
    assertEquals(1, cmp.compare(t[0], t[3]));
    assertEquals(-1, cmp.compare(t[2], t[4]));
    assertEquals(1, cmp.compare(t[4], t[2]));
    assertEquals(-1, cmp.compare(t[3], t[4]));
    assertEquals(1, cmp.compare(t[4], t[3]));
    assertEquals(0, cmp.compare(t[4], t[5]));
    
    assertTrue(cmp.match(t[0], t[1]));
    assertTrue(!cmp.match(t[0], t[2]));
    assertTrue(cmp.match(t[0], t[3]));
    assertTrue(cmp.match(t[2], t[3]));
    assertTrue(!cmp.match(t[3], t[4]));
    assertTrue(cmp.match(t[4], t[5]));
    assertTrue(cmp.match(t[5], t[6]));

  }
  
  
  @Test
  public void testScatteredTriple() {
    
    byte[] buf = "   TestSubject TestPredicate TestObject   ".getBytes();
    Triple t = Triple.newInstance(buf, 3, (short) 11, 15, (short) 13, 29, 10, 1);
    
    Triple c = Triple.newTriple("TestSubject", "TestPredicate", "TestObject", 1);
    assertEquals(t, c);
    assertEquals(c, t.copy());
    assertEquals(c, t.clone());
  }
  
  
  @Test
  public void testMagicTriple() {
    assertTrue(Triple.MAGIC_TRIPLE.isMagic());
  }
  
  
  @Test
  public void testClipping() {
    Triple t = Triple.newTriple("12345", "67", "89");
    assertEquals(t.clip(Triple.COLLATION.SPO, 20), t);
    assertEquals(t.clip(Triple.COLLATION.SPO, 5), Triple.newTriple("12345", "", ""));
    assertEquals(t.clip(Triple.COLLATION.SPO, 2), Triple.newTriple("12", "", ""));
    assertEquals(t.clip(Triple.COLLATION.SPO, 8), Triple.newTriple("12345", "67", "8"));
  }
  
  
  @Test
  public void testEquals() {
    assertEquals(Triple.newTriple("s", "p", "o"), Triple.newTriple("s", "p", "o"));
    assertEquals(Triple.COLLATION.SPO.mask(Triple.newTriple("s", "p", "o"), false), 
        Triple.newPattern("s", "p", null));
    assertFalse(Triple.COLLATION.SPO.mask(Triple.newTriple("s", "p", "o"), false).equals(
        Triple.newTriple("s", "p", "o")));
    assertEquals(Triple.COLLATION.SPO.mask(Triple.newTriple("s", "p", "o"), true), 
        Triple.newPattern("s", null, null));
  }
  
}
