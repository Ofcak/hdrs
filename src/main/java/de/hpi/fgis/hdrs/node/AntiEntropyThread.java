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

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.Peer;
import de.hpi.fgis.hdrs.routing.Router;

public class AntiEntropyThread extends Thread {

  static final Log LOG = LogFactory.getLog(AntiEntropyThread.class);
  
  private final Router router;
  private volatile boolean quit = false;
  
  LinkedBlockingQueue<Peer> queue = new LinkedBlockingQueue<Peer>();
  
  AntiEntropyThread(Router router) {
    this.router = router;
    setName("Anti-Entropy Thread");
  }
  
  @Override 
  public void run() {
    
    while (!quit) {
    
      // wait for some ae request, or 10 seconds
      Peer peer = null;
      try {
        peer = queue.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // ignore
      }
      
      try {
        router.update(peer);
      } catch (UnknownHostException ex) {
        // this node can't be reached.  might be a temporary network failure,
        // log a warning
        LOG.warn("Cannot run anti-entropy with peer " + peer + ": unkown host");
      } catch (ConnectException ex) {
        LOG.warn("Cannot run anti-entropy with peer " + peer + ": cannot connect");
      } catch (IOException ex) {
        LOG.warn("Anti-entropy error.", ex);
      } catch (Throwable ex) {
        LOG.fatal("Anti-entropy failure.", ex);
        quit();
      }
      
    }
    
  }
  
  public void quit() {
    quit = true;
    interrupt();
  }
  
  
  public void quitJoin() {
    quit();
    try {
      join();
    } catch (InterruptedException e) {
      // ignore
    }
  }
  
  
  public void run(Peer peer) {
    queue.offer(peer);
  }
  
}
