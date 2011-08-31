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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class TripleFileTest {

  @Test
  public void testWriteAndRead() throws IOException {
    writeAndRead(Triple.COLLATION.SPO, Compression.ALGORITHM.NONE);
  }
  
  @Test
  public void testWriteAndReadDeflate() throws IOException {
    writeAndRead(Triple.COLLATION.SPO, Compression.ALGORITHM.DEFLATE);
  }
  
  @Test
  public void testWriteAndReadSnappy() throws IOException {
    writeAndRead(Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
  }
  
  private void writeAndRead(Triple.COLLATION order, Compression.ALGORITHM compression) 
      throws IOException {
        
    File file = File.createTempFile("hdrs_test", "");
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = generateTriples(numTriples);
      
    writeToFile(triples, file.getAbsolutePath(), order, compression);
    
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();
    
    TripleScanner scanner = reader.getScanner();
    
    Comparator<Triple> comparator = reader.getOrder().comparator();
    
    Iterator<Triple> compare = triples.iterator();
    while (scanner.next()) {
      Triple t1 = scanner.pop();
      Triple t2 = compare.next();
      assertTrue(0 == comparator.compare(t1, t2));
    }
    assertTrue(!compare.hasNext());
    
    scanner.close();
    reader.close();
    file.deleteOnExit();
  }
  
  
  @Test
  public void testSeekedScanner() throws IOException {
    testSeekedScanner(Triple.COLLATION.SPO, Compression.ALGORITHM.NONE);
  }
  
  @Test
  public void testSeekedScannerDeflate() throws IOException {
    testSeekedScanner(Triple.COLLATION.SPO, Compression.ALGORITHM.DEFLATE);
  }
  
  @Test
  public void testSeekedScannerSnappy() throws IOException {
    testSeekedScanner(Triple.COLLATION.SPO, Compression.ALGORITHM.SNAPPY);
  }
  
  private void testSeekedScanner(Triple.COLLATION order, Compression.ALGORITHM compression) 
      throws IOException {
    
    File file = File.createTempFile("hdrs_test", "");
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = generateTriples(numTriples);
    writeToFile(triples, file.getAbsolutePath(), order, compression);
    Triple[] tarray = triples.toArray(new Triple[0]);
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();    
    Comparator<Triple> comparator = reader.getOrder().comparator();
    TripleScanner scanner;
    
    
    scanner = reader.getScanner(tarray[0]);
    assertTrue(0 == comparator.compare(tarray[0], scanner.peek()));
    
    scanner = reader.getScanner(tarray[500]);
    assertTrue(0 == comparator.compare(tarray[500], scanner.peek()));
    
    scanner = reader.getScanner(tarray[5000]);
    assertTrue(0 == comparator.compare(tarray[5000], scanner.peek()));
    
    scanner = reader.getScanner(tarray[numTriples-1]);
    assertEquals(0, comparator.compare(tarray[numTriples-1], scanner.peek()));
    
    assertTrue(null == reader.getScannerAt(Triple.newTriple("not", "in", "file", (short) 1)));
    assertTrue(null != reader.getScanner(Triple.newTriple("not", "in", "file", (short) 1)));
    
    reader.close();
    file.deleteOnExit();
  }
  
  
  public static NavigableSet<Triple> generateTriples(int amount) {
    NavigableSet<Triple> tree = new TreeSet<Triple>(Triple.COLLATION.SPO.comparator());
    Random rnd = new Random();
    rnd.setSeed(System.currentTimeMillis());
    for (int i=0; i<amount; i++) {
      int num = rnd.nextInt();
      tree.add(Triple.newTriple("s"+num, "pred", "obj", (short) 1));
    }
    return tree;
  }
  
  public static void writeToFile(NavigableSet<Triple> triples, String filename, 
      Triple.COLLATION order, Compression.ALGORITHM compression) throws IOException {
    TripleFileWriter writer = new TripleFileWriter(filename, order, compression);
    for (Triple t : triples) {
      writer.appendTriple(t);
    }
    writer.close();
  }
  
  
  @Test
  public void testBlockIndexSearch() {
    
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
    
    Comparator<Triple> cmp = Triple.COLLATION.SPO.comparator();
    
    assertTrue(0 == BlockIndex.binarySearch(triples,
        Triple.newTriple("A", "A", "B", (short) 0), cmp));
    assertTrue(2 == BlockIndex.binarySearch(triples, // 1??
        Triple.newTriple("B", "A", "A", (short) 0), cmp));
    assertTrue(7 == BlockIndex.binarySearch(triples, // 6??
        Triple.newTriple("G", "A", "A", (short) 0), cmp));
    assertTrue(9 == BlockIndex.binarySearch(triples, // 8??
        Triple.newTriple("H", "A", "A", (short) 0), cmp));
    
    // out of index
    assertTrue(-1 == BlockIndex.binarySearch(triples, 
        Triple.newTriple("", "", "", (short) 0), cmp));    //0
    assertTrue(-1 == BlockIndex.binarySearch(triples, 
        Triple.newTriple("A", "A", "A", (short) 0), cmp));  //0
    assertTrue(9 == BlockIndex.binarySearch(triples, 
        Triple.newTriple("H", "B", "A", (short) 0), cmp));
    
    // not indexed
    assertTrue(0 == BlockIndex.binarySearch(triples, 
        Triple.newTriple("A", "A", "C", (short) 0), cmp));
    assertTrue(4 == BlockIndex.binarySearch(triples, 
        Triple.newTriple("B", "B", "A", (short) 0), cmp)); //5
    assertTrue(8 == BlockIndex.binarySearch(triples, 
        Triple.newTriple("G", "A", "B", (short) 0), cmp));

    // wildcard search
    assertTrue(-1 == BlockIndex.binarySearch(triples, 
        Triple.newPattern(null, null, null), cmp));
    assertTrue(1 == BlockIndex.binarySearch(triples, 
        Triple.newPattern("B", "A", null), cmp));
    assertTrue(6 == BlockIndex.binarySearch(triples, 
        Triple.newPattern("G", null, null), cmp));
    
  }
  
}
