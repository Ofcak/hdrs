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

public interface SeekableTripleSource extends OrderedTripleSource {

  /**
   * Get a scanner that is seeked to the first triple matching <pattern>.
   * If there is no such triple, the scanner will be positioned at the 
   * first triple greater than <patterns>.
   * @param pattern
   * @return
   * @throws IOException
   */
  public TripleScanner getScanner(Triple pattern) throws IOException;
  
  /**
   * Same as getScanner(), except that this method returns null if there
   * is no matching triple for <pattern>.
   * @param pattern
   * @return
   * @throws IOException
   */
  public TripleScanner getScannerAt(Triple pattern) throws IOException;
  
  public TripleScanner getScannerAfter(Triple pattern) throws IOException;
  
}
