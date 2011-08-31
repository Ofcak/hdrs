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


public class SegmentSplitThread extends SegmentServiceThread {

  // segment is not writable/readable while in SPLIT_PENDING
  // --> aggressive polling on this one.
  public static final long SPLIT_RETRY_DELAY = 500;
  
  public SegmentSplitThread(SegmentServer server) {
    super(server);
    setName("Segment Split Thread");
  }

  @Override
  protected void handle(long segmentId) {
    try {
      if (!server.splitSegment(segmentId)) {
        request(segmentId, SPLIT_RETRY_DELAY);
      }
    } catch (IOException ex) {
      LOG.error("Segment split failed for segment " + segmentId + ".", ex);
      fail(ex);
    }
  }

}
