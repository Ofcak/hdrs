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

package de.hpi.fgis.hdrs.tio;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;

/**
 * The /dev/null of triple sinks.
 * 
 * @author daniel.hefenbrock
 *
 */
public class NullSink implements OrderedTripleSink {

  private final Triple.COLLATION order;
  private long nBytesWritten = 0;
  
  public NullSink() {
    this.order = Triple.COLLATION.SPO;
  }
  
  public NullSink(Triple.COLLATION order) {
    this.order = order;
  }
  
  @Override
  public boolean add(Triple triple) {
    nBytesWritten += triple.estimateSize();
    return true;
  }

  @Override
  public COLLATION getOrder() {
    return order;
  }

  public void resetCounter() {
    nBytesWritten = 0;
  }
  
  public long written() {
    return nBytesWritten;
  }
  
}
