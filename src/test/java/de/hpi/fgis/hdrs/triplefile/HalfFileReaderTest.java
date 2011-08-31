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
import java.util.NavigableSet;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.tio.TripleScanner;
import de.hpi.fgis.hdrs.compression.Compression;


public class HalfFileReaderTest {

  
  @Test
  public void testHalfFileReader() throws IOException {
    
    File file = File.createTempFile("hdrs_test", "");
    file.deleteOnExit();
    
    int numTriples = 10000;
  
    NavigableSet<Triple> triples = TripleFileTest.generateTriples(numTriples);
      
    TripleFileTest.writeToFile(triples, file.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.DEFLATE);
    
    TripleFileReader reader = new TripleFileReader(file.getAbsolutePath());
    reader.open();
    
    
    testHalfFileReader(reader.getScanner(), file.getAbsolutePath(), 
        reader.getSplitTriple());
    
    // get another split triple that is not the first triple of some block
    TripleScanner scanner = reader.getScanner(reader.getSplitTriple());
    for (int i=0; i<13; ++i) {
      scanner.next(); scanner.pop();
    }
    scanner.next();
    
    testHalfFileReader(reader.getScanner(), file.getAbsolutePath(), 
        scanner.pop());
    
    scanner.close();
    
    reader.close();
  }
  
  private void testHalfFileReader(TripleScanner scanner, String triplefile,
      Triple splitTriple) throws IOException {
    HalfFileReader topReader = new HalfFileReader(triplefile,
        splitTriple, HalfFileReader.HALF.TOP);
    topReader.open();
    
    HalfFileReader bottomReader = new HalfFileReader(triplefile,
        splitTriple, HalfFileReader.HALF.BOTTOM);
    bottomReader.open();
        
    TripleScanner hscanner = topReader.getScanner();
    while (hscanner.next()) {
      assertTrue(scanner.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          hscanner.pop(), scanner.pop()));
    }
    
    hscanner = bottomReader.getScanner();
    while (hscanner.next()) {
      assertTrue(scanner.next());
      assertEquals(0, Triple.COLLATION.SPO.comparator().compare(
          hscanner.pop(), scanner.pop()));
    }
    
    assertTrue(!scanner.next());
    
    hscanner.close();
    scanner.close();
    
    topReader.close();
    bottomReader.close();
  }
  
}
