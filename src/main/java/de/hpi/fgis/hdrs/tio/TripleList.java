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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.hpi.fgis.hdrs.Triple;

public class TripleList implements TripleSink, TripleSource {

  private final List<Triple> triples = new ArrayList<Triple>();
  
  @Override
  public boolean add(Triple triple) throws IOException {
    return triples.add(triple);
  }

  @Override
  public TripleScanner getScanner() throws IOException {
    return new IteratorScanner(triples.iterator());
  }
  
  public void sort(Triple.COLLATION order) {
    Collections.sort(triples, order.comparator());
  }

}
