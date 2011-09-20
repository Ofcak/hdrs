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

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.segment.SegmentService;


public abstract class SegmentServiceThread extends Thread implements SegmentService {

  static final Log LOG = LogFactory.getLog(SegmentServiceThread.class);
  
  /**
   * After this timeout the service thread will get an opportunity
   * to do some maintenance work. (every 5 minutes)
   */
  public static final int POLL_TIMEOUT = 5 * 60 * 1000;
    
  
  protected final SegmentServer server;
  
  private final DelayQueue<DelayQueueEntry> queue;
  
  private boolean quit = false;
  
  
  public SegmentServiceThread(SegmentServer server) {
    this.server = server;
    queue = new DelayQueue<DelayQueueEntry>();
  }
  
  
  @Override
  public void run() {
    
    while (!quit) {
      // get next segment to flush
      DelayQueueEntry entry = null;
      long segmentId = 0;
      try {
        entry = queue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);
        
        if (null != entry) {
          segmentId = entry.getId();
          handle(segmentId);
        } else {
          handle();
        }
        
      } catch (InterruptedException ex) {
        // interrupted... 
      } catch (Throwable unchecked) {
        fail(unchecked);
      }
    }
    
    LOG.info("Quitting.");
  }
  
  
  abstract protected void handle(long segmentId);
  
  /**
   * Handle timeout.  Default is NOOP.
   */
  protected void handle() {
  }
  
  
  public void request(long segmentId, long delay) {
    queue.add(new DelayQueueEntry(segmentId, delay, false));
  }
  
  
  @Override
  public void request(long segmentId) {
    request(segmentId, 0);
  }
  
  
  public void requestUrgent(long segmentId) {
    DelayQueueEntry request = new DelayQueueEntry(segmentId, 0, true);
    queue.add(request);
  }
  
  
  @Override
  public boolean removeRequest(long segmentId) {
    return queue.remove(new DelayQueueEntry(segmentId));
  }
  
  
  public void quit() {
    quit = true;
    this.interrupt();
  }
  
  
  public void quitJoin() {
    quit();
    try {
      join();
    } catch (InterruptedException e) {
      // ignore
    }
  }
  
  
  protected void fail(Throwable reason) {
    server.stop(reason);
    quit = true;
  }
 
  
  private static class DelayQueueEntry implements Delayed {

    private final long id;
    private final long deadline;
    private final boolean urgent;
    
    public DelayQueueEntry(long id, long delayMillis, boolean urgent) {
      this.id = id;
      this.deadline = System.currentTimeMillis() + delayMillis;
      this.urgent = urgent;
    }
    
    // only used for remove
    DelayQueueEntry(long id) {
      this.id = id;
      this.deadline = 0;
      this.urgent = false;
    }
    
    public long getId() {
      return id;
    }
    
    public boolean getUrgent() {
      return urgent;
    }
    
    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed that) {
      return (int) (getDelay(TimeUnit.MILLISECONDS) - that.getDelay(TimeUnit.MILLISECONDS));
    }
    
    @Override
    public int hashCode() {
      return (int)(id ^ (id >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      DelayQueueEntry other = (DelayQueueEntry) obj;
      return id == other.id;
    }
    
  }
}
