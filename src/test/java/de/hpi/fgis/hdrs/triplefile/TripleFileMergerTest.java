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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.NavigableSet;

import org.junit.Test;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.compression.Compression;
import de.hpi.fgis.hdrs.tio.TripleScanner;


public class TripleFileMergerTest {

  
  @Test
  public void testFileMerger() throws IOException {
    
    File file1 = File.createTempFile("hdrs_test", "");
    File file2 = File.createTempFile("hdrs_test", "");
    
    NavigableSet<Triple> triples;
    
    triples = TripleFileTest.generateTriples(1000);
    TripleFileTest.writeToFile(triples, file1.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.DEFLATE);
    
    triples = TripleFileTest.generateTriples(1000);
    TripleFileTest.writeToFile(triples, file2.getAbsolutePath(), 
        Triple.COLLATION.SPO, Compression.ALGORITHM.DEFLATE);
    
    File out = File.createTempFile("hdrs_test", "");
    out.deleteOnExit();
            
    TripleFileUtils.merge(out.getAbsolutePath(), file1.getAbsolutePath(),
        file2.getAbsolutePath());
    
    TripleFileReader reader = new TripleFileReader(out.getAbsolutePath());
    reader.open();
    TripleScanner scanner = reader.getScanner();
    
    assertTrue(scanner.next());
    Triple t1 = scanner.pop();
    while(scanner.next()) {
      Triple t2 = scanner.pop();
      assertTrue(0 > Triple.COLLATION.SPO.comparator().compare(t1, t2));
      t1 = t2;
    }
    scanner.close();
    
    
    file1.deleteOnExit();
    file2.deleteOnExit();
  }
  
}
