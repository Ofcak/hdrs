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

import java.io.IOException;
import java.util.Iterator;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;

public class IteratorScanner extends TripleScanner {

  private final Iterator<Triple> iterator;
  
  public IteratorScanner(Iterator<Triple> it) {
    iterator = it;
  }
  
  @Override
  public COLLATION getOrder() {
    return null;
  }

  @Override
  protected Triple nextInternal() throws IOException {
    if (!iterator.hasNext()) {
      return null;
    }
    return iterator.next();
  }

  @Override
  public void close() throws IOException {
  }
  
}
