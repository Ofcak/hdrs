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

package de.hpi.fgis.hdrs.parser;

import java.io.UnsupportedEncodingException;

import de.hpi.fgis.hdrs.Triple;

public class BTCParser implements TripleParser<String> {

  private boolean skipContext = false;
  
  @Override
  public Triple parse(String input) {
    try {
      return skipContext ? parseTripleSkipContext(input) : parseTriple(input);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  public void setSkipContext(boolean skip) {
    skipContext = skip;
  }
  
  
  public static Triple parseTripleSkipContext(String line) throws UnsupportedEncodingException {
    line = line.trim();
    line = line.substring(0, line.length()-1).trim();
    int pos1 = line.indexOf(' ');
    int pos2 = line.indexOf(' ', pos1+1);
    
    byte[] bytes = line.getBytes("US-ASCII");
    
    byte[] buf = new byte[bytes.length-2];
    
    System.arraycopy(bytes, 0, buf, 0, pos1);
    System.arraycopy(bytes, pos1+1, buf, pos1, pos2-pos1-1);

    int len = bytes.length;
    if ('>' == bytes[bytes.length-1]) {
      int last = line.lastIndexOf(' ');
      if (last != pos2) {
        len = last;
      }
    }
    
    System.arraycopy(bytes, pos2+1, buf, pos2-1, len-pos2-1);
    
    return Triple.newInstance(
        buf, 0,
        (short) pos1, (short) (pos2-pos1-1), len-pos2-1, 1);
  }
  
  
  public static Triple parseTriple(String line) throws UnsupportedEncodingException {
    int pos1 = line.indexOf(' ');
    int pos2 = line.indexOf(' ', pos1+1);
    
    byte[] bytes = line.getBytes("US-ASCII");
    
    byte[] buf = new byte[bytes.length-4];
    
    System.arraycopy(bytes, 0, buf, 0, pos1);
    System.arraycopy(bytes, pos1+1, buf, pos1, pos2-pos1-1);
    System.arraycopy(bytes, pos2+1, buf, pos2-1, bytes.length-pos2-3);
    
    return Triple.newInstance(
        buf, 0,
        (short) pos1, (short) (pos2-pos1-1), bytes.length-pos2-3, 1);
  }
  
  
}
