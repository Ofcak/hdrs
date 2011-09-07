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

import java.io.IOException;

import org.junit.Test;

import de.hpi.fgis.hdrs.client.Client;
import de.hpi.fgis.hdrs.client.TripleOutputStream;
import de.hpi.fgis.hdrs.node.Node;
import de.hpi.fgis.hdrs.node.NodeLauncher;
import de.hpi.fgis.hdrs.tio.AggregatorScanner;
import de.hpi.fgis.hdrs.tio.TripleGenerator;
import de.hpi.fgis.hdrs.tio.TripleList;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class WriteReadTest {

  @Test
  public void testReadWriteNode() throws IOException, InterruptedException {
    
    TripleList triples = new TripleList();
    TripleGenerator gen = new TripleGenerator();
    for (int i=0; i<100000; ++i) {
      triples.add(gen.generate());
    }
    
    testReadWriteNode(triples);
  }
  
  
  public void testReadWriteNode(TripleList triples) throws IOException, InterruptedException {
       
    Configuration nConf = Configuration.create();
    nConf.setInt(Configuration.KEY_SEGMENT_BUFFER_SIZE, 512 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_FLUSH_THRESHOLD, 256 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_SPLIT_SIZE, 128 * 1024);
    
    Node node = NodeLauncher.createTmpNode(nConf, 51337, Triple.COLLATION.SPO);
    Thread nodeThread = new Thread(node);
    nodeThread.start();
    
    node.waitOnline();
    
    Configuration conf = Configuration.create();
    
    Client client = node.getClient(conf);
    
    TripleScanner fs = triples.getScanner();
    
    TripleOutputStream out = client.getOutputStream();
    while (fs.next()) {
      out.add(fs.pop());
    }
    out.close();
    fs.close();
    
    triples.sort(Triple.COLLATION.SPO);
    Thread.sleep(10000);
    
    fs = new AggregatorScanner(triples.getScanner());
    TripleScanner ss = client.getScanner(client.getIndexes().iterator().next());
    assertTrue(null != ss);
    
    while (fs.next()) {
      assertTrue(ss.next());
      assertEquals(fs.pop(), ss.pop());
    }
    assertFalse(ss.next());
    
    fs.close();
    ss.close();
    
    client.close();
    
    Thread.sleep(1000);
      
    node.shutDown();
    nodeThread.join();
    
    node.delete();
  }
  
  
  @Test
  public void testWriteDeleteNode() throws IOException, InterruptedException {
    
    TripleList triples = new TripleList();
    TripleGenerator gen = new TripleGenerator();
    for (int i=0; i<100000; ++i) {
      triples.add(gen.generate());
    }
    
    Configuration nConf = Configuration.create();
    nConf.setInt(Configuration.KEY_SEGMENT_BUFFER_SIZE, 512 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_FLUSH_THRESHOLD, 256 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_SPLIT_SIZE, 128 * 1024);
    
    Node node = NodeLauncher.createTmpNode(nConf, 51337, Triple.COLLATION.SPO);
    Thread nodeThread = new Thread(node);
    nodeThread.start();
    
    node.waitOnline();
    
    Configuration conf = Configuration.create();
    
    Client client = node.getClient(conf);
    
    TripleScanner fs = triples.getScanner();
    
    TripleOutputStream out = client.getOutputStream();
    while (fs.next()) {
      out.add(fs.pop());
    }
    out.close();
    fs.close();
    
    fs = triples.getScanner();
    out = client.getOutputStream();
    while (fs.next()) {
      out.add(fs.pop().getDelete());
    }
    out.close();
    fs.close();
    
    Thread.sleep(1000);
    node.requestFlush(0);
    Thread.sleep(2000);
    node.requestCompaction(0);
    Thread.sleep(2000);
    
    TripleScanner ss = client.getScanner(client.getIndexes().iterator().next());
    try {
      assertTrue(null == ss || !ss.next());
    } finally {
    
      client.close();
    
      Thread.sleep(1000);
      
      node.shutDown();
      nodeThread.join();
    
      node.delete();
    }
  }
  
  
  @Test
  public void testConcurrentReadWriteNode() throws Throwable {
    
    Configuration nConf = Configuration.create();
    nConf.setInt(Configuration.KEY_SEGMENT_BUFFER_SIZE, 512 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_FLUSH_THRESHOLD, 256 * 1024);
    nConf.setInt(Configuration.KEY_SEGMENT_SPLIT_SIZE, 128 * 1024);
    
    Node node = NodeLauncher.createTmpNode(nConf, 51337, Triple.COLLATION.POS);
    Thread nodeThread = new Thread(node);
    nodeThread.start();
    
    node.waitOnline();
    
    Client wClient = node.getClient(Configuration.create());
    Writer writer = new Writer(wClient);
    writer.start();
   
    Client rClient = node.getClient(Configuration.create());
    Reader reader = new Reader(rClient);
    reader.start();
    
    
    writer.join();
    if (null != writer.getException()) {
      throw writer.getException();
    }
    reader.quit();
    reader.join();
    if (null != reader.getException()) {
      throw reader.getException();
    }
    
    wClient.close();
    rClient.close();
    
    Thread.sleep(1000);
      
    node.shutDown();
    nodeThread.join();
    
    node.delete();
  }
  
  
  static class Writer extends Thread {
    
    private final Client client;
    private volatile Throwable t = null;
    
    Writer(Client client) {
      this.client = client;
    }
    
    @Override
    public void run() {
      TripleOutputStream out = client.getOutputStream();
      TripleGenerator gen = new TripleGenerator();
      try {
        for (int i=0; i<100000; ++i) {
          out.add(gen.generate());
        }
        out.close();
      } catch (Throwable e) {
        t = e;
      } 
    }
    
    Throwable getException() {
      return t;
    }
    
  }
  
  static class Reader extends Thread {
    
    private final Client client;
    private volatile Throwable t = null;
    private volatile boolean quit = false;
    
    Reader(Client client) {
      this.client = client;
    }
    
    @Override
    public void run() {
      try {
        while (!quit) {
          TripleScanner ss = client.getScanner(client.getIndexes().iterator().next());
          while (ss.next() && !quit) {
            ss.pop();
          }
          ss.close();
        }
      } catch (IOException e) {
        t = e;
      }
    }
    
    Throwable getException() {
      return t;
    }
    
    void quit() {
      quit = true;
    }
    
  }

}
