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
import java.util.Collection;
import java.util.Iterator;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;

/**
 * A util class that provides a scanner on top of a collection of
 * triple sources.
 * 
 * Does not enforce/guarantee ordering.
 * 
 * @author daniel.hefenbrock
 *
 */
public class MultiSourceScanner extends TripleScanner {

  final private Iterator<? extends TripleSource> sources;
  private TripleScanner scanner = null;
  
  public MultiSourceScanner(Collection<? extends TripleSource> sources) {
    this.sources = sources.iterator();
  }
  
  @Override
  public COLLATION getOrder() {
    return null;
  }

  @Override
  protected Triple nextInternal() throws IOException {
    while (null == scanner || !scanner.next()) {
      if (!sources.hasNext()) {
        return null;
      }
      scanner = sources.next().getScanner();
    }
    return scanner.pop();
  }

  @Override
  public void close() throws IOException {
    if (null != scanner) {
      scanner.close();
    }
  }
  
}
