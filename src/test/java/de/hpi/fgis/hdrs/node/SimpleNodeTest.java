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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.ipc.NodeProtocol;
import de.hpi.fgis.hdrs.ipc.MultiSegmentWrite;
import de.hpi.fgis.hdrs.ipc.TransactionState;
import de.hpi.fgis.hdrs.routing.SegmentInfo;

/**
 * Some very basic tests.  No multi threading.
 * @author daniel.hefenbrock
 *
 */
public class SimpleNodeTest {

  Node node;
  
  @Before
  public void setUp() throws IOException {
    node = NodeLauncher.createTmpNode(Configuration.create(), 51337, Triple.COLLATION.SPO);
    node.initialize();
  }
  
  @After
  public void tearDown() throws IOException {
    node.close();
    node.delete();
    if (!node.rootDir.delete()) {
      throw new IOException("couldn't delete node tmp dir");
    }
  }
  
  
  @Test
  public void testSetupTearDown() {
    assertEquals(1, node.getLocalSegmentCatalog().getSegments(
        Triple.COLLATION.SPO).size());
  }
  
  
  @Test
  public void testEmptyTransaction() throws IOException {
    node.startTransaction(1, 1024, 10000);
    node.prepareTransaction(1);
    node.commitTransaction(1);
  }
  
  
  @Test
  public void testSimpleWriteTransaction() throws IOException {
    SegmentInfo info = node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(0);
    node.startTransaction(1, 1024, 10000);
    node.writeTriples(1, getTriple(info.getSegmentId()));
    node.prepareTransaction(1);
    node.commitTransaction(1);
  }
  
  
  @Test
  public void testSimpleAbortTransaction() throws IOException {
    SegmentInfo info = node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(0);
    node.startTransaction(1, 1024, 10000);
    node.writeTriples(1, getTriple(info.getSegmentId()));
    node.abortTransaction(1);
  }
  
  
  @Test
  public void testAutoAbortTransaction() throws IOException {
    SegmentInfo info = node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(0);
    node.startTransaction(1, 1024, 10000);
    node.writeTriples(1, getTriple(info.getSegmentId()));
  }
  
  
  @Test
  public void testPreparedAbortTransaction() throws IOException {
    SegmentInfo info = node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(0);
    node.startTransaction(1, 1024, 10000);
    node.writeTriples(1, getTriple(info.getSegmentId()));
    node.prepareTransaction(1);
    node.abortTransaction(1);
  }
  
  
  @Test
  public void testSegmentSplit() throws IOException {
    SegmentInfo info = node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(0);
    
    // run 100 transactions
    for (int i=0; i<100; ++i) {
      assertEquals(NodeProtocol.TRUE, node.startTransaction(i, 256*1024, 10000));
      
      MultiSegmentWrite triples = getTriples(1000, info.getSegmentId());
      assertTrue(((TransactionState) node.writeTriples(i, triples)).isOk());
      
      assertEquals(TransactionState.OK, node.prepareTransaction(i));
      
      node.commitTransaction(i);
    }
    
    assertTrue(node.splitSegment(info.getSegmentId()));
    
    node.getRouter().update(null);
    
    node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(0);
    node.getLocalSegmentCatalog().getSegments(Triple.COLLATION.SPO).get(1);
  }
  
  
  static MultiSegmentWrite getTriple(long segmentId) throws IOException {
    return getTriples(1, segmentId);
  }
  
  static MultiSegmentWrite getTriples(int n, long segmentId) throws IOException {
    Random rnd = new Random(System.currentTimeMillis());
    MultiSegmentWrite writes = new MultiSegmentWrite();
    for (int i=0; i<n; ++i) {
      Triple t = Triple.newTriple("testA"+rnd.nextInt(), "testB", "testC");
      writes.add(segmentId, Triple.COLLATION.SPO, t);
    }
    return writes;
  }
  
}
