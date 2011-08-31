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

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;

public class AggregatorScanner extends TripleScanner {

  private final TripleScanner scanner;
  
  public AggregatorScanner(TripleScanner scanner) {
    this.scanner = scanner;
  }
  
  @Override
  public COLLATION getOrder() {
    return scanner.getOrder();
  }

  @Override
  protected Triple nextInternal() throws IOException {
    if (!scanner.next()) {
      return null;
    }
    Triple t = scanner.pop();
    while (scanner.next() && t.equals(scanner.peek())) {
      t.merge(scanner.pop());
    }
    return t;
  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }

}
