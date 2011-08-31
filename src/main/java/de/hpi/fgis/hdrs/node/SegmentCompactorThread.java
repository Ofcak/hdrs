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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.segment.SegmentService;



public class SegmentCompactorThread extends Thread implements SegmentService {
    
  static final Log LOG = LogFactory.getLog(SegmentCompactorThread.class);
  
  public SegmentCompactorThread(SegmentServer server) {
    this.server = server;
    setName("Segment Compactor Thread");
  }
  
  private final SegmentServer server;
  
  private final Semaphore available = new Semaphore(0);
  private final Set<CompactionRequest> segments = new HashSet<CompactionRequest>();
  
  private boolean quit = false;
  
  
  @Override
  public void run() {
    
    while (!quit) {
      
      try {
        
        // wait for work
        available.acquire();
        
        int removed = server.compactSegment(segments);
        
        if (1 < removed) {
          available.acquire(removed - 1);
        }
        
      } catch (InterruptedException ex) {
        // ignore
      } catch (Throwable ex) {
        // something went wrong while compacting this segment.
        LOG.fatal("Compaction failed", ex);
        fail(ex);
      }
    }
    
    LOG.info("Quitting.");
  }
  

  @Override
  public boolean removeRequest(long segmentId) {
    synchronized (segments) {
      if (segments.remove(new CompactionRequest(segmentId, false))) {
        if (!available.tryAcquire()) {
          LOG.warn("tryAcquire() failed when removing compaction request");
        }
        return true;
      }
      return false;
    }
  }


  @Override
  public void request(long segmentId) {
    request(segmentId, false);
  }
  
  
  @Override
  public void requestUrgent(long segmentId) {
    request(segmentId, true);
  }
  
  
  public void request(long segmentId, boolean major) {
    synchronized (segments) {
      if (segments.add(new CompactionRequest(segmentId, major))) {
        available.release();
      }
    }
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
  
  
  public static class CompactionRequest {
    
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (segmentId ^ (segmentId >>> 32));
      return result;
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
      CompactionRequest other = (CompactionRequest) obj;
      if (segmentId != other.segmentId) {
        return false;
      }
      return true;
    }

    final long segmentId;
    final boolean major;
    
    CompactionRequest(long segmentId, boolean major) {
      this.segmentId = segmentId;
      this.major = major;
    }
    
    public long getSegmentId() {
      return segmentId;
    }
    
    public boolean isMajor() {
      return major;
    }
    
    
  }
  
}
