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


public class SegmentFlusherThread extends SegmentServiceThread {
  
  public static final long FLUSH_RETRY_DELAY = 5 * 1000;
  
  
  public SegmentFlusherThread(SegmentServer server) {
    super(server);
    setName("Flush Thread");
  }
  
  
  @Override
  protected void handle(long segmentId) {
    try {
      if (!server.flushSegment(segmentId)) {
        request(segmentId, FLUSH_RETRY_DELAY);
      }
    } catch (IOException ex) {
      LOG.fatal("Buffer flush failed for segment " + segmentId + ".", ex);
      fail(ex);
    }
  }
  
  
  @Override 
  protected void handle() {
    server.flushMaintenance();
  }
  
  
}
