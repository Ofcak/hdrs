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

package de.hpi.fgis.hdrs.client;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.node.Node;
import de.hpi.fgis.hdrs.node.NodeLauncher;

public class ClientTest {

  private Node node;
  private Client client;
  
  @Before
  public void setUp() throws IOException {
    Configuration nConf = Configuration.create();
    nConf.setInt(Configuration.KEY_SEGMENT_BUFFER_SIZE, 512 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_FLUSH_THRESHOLD, 256 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_SPLIT_SIZE, 128 * 1024);
    
    node = NodeLauncher.createTmpNode(nConf, 51337, Triple.COLLATION.SPO);
    Thread nodeThread = new Thread(node);
    nodeThread.start();
    
    node.waitOnline();
    
    Configuration conf = Configuration.create();
    
    client = node.getClient(conf);
  }
  
  @After
  public void tearDown() throws IOException {
    
    client.close();
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
      
    node.shutDown();
    node.waitDone();
    
    node.delete(); 
  }  
  
  @Test
  public void testWriteConstant() throws IOException {
    
    TripleOutputStream out = client.getOutputStream();
    
    for (int i=0; i<1000; ++i) {
      out.add(Triple.newTriple("subject", "predicate", "object"));
    }
    
    out.close();
    
  }
  
}
