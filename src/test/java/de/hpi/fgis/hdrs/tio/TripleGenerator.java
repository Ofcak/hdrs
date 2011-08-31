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
import java.util.Random;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;

public class TripleGenerator implements TripleSource {

  private final Random rnd;
  
  public TripleGenerator() {
    rnd = new Random(System.currentTimeMillis());
  }
  
  @Override
  public TripleScanner getScanner() throws IOException {
    return new TripleScanner() {

      @Override
      public COLLATION getOrder() {
        throw new UnsupportedOperationException();
      }

      @Override
      protected Triple nextInternal() throws IOException {
        return TripleGenerator.this.generate();
      }

      @Override
      public void close() throws IOException {
      }
      
    };
  }
  
  public Triple generate() {
    int num = rnd.nextInt();
    return Triple.newTriple("s"+num, "pred", "obj");
  }
  
  /**
   * Use this in order to avoid collisions with other generators.
   */
  public Triple generate(String postfix) {
    int num = rnd.nextInt();
    return Triple.newTriple("s"+num+postfix, "pred", "obj");
  }

}
