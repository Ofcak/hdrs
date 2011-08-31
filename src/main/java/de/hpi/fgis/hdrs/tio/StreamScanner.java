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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.Triple.COLLATION;
import de.hpi.fgis.hdrs.parser.TripleParser;
import de.hpi.fgis.hdrs.parser.TripleParserFactory;

public class StreamScanner extends TripleScanner {

  public static final int BUFFER_SIZE = 64 * 1024;
  
  private final BufferedReader in;
  private final TripleParser<String> parser;
  
  public StreamScanner(InputStream in) {
    this (in, TripleParserFactory.getDefaultLineParser());
  }
  
  public StreamScanner(Reader in) {
    this (in, TripleParserFactory.getDefaultLineParser());
  }
  
  public StreamScanner(InputStream in, TripleParser<String> parser) {
    this.in = new BufferedReader(new InputStreamReader(in), BUFFER_SIZE);
    this.parser = parser;
  }
  
  public StreamScanner(Reader in, TripleParser<String> parser) {
    this.in = new BufferedReader(in, BUFFER_SIZE);
    this.parser = parser;
  }
  
  @Override
  public COLLATION getOrder() {
    return null;
  }

  @Override
  protected Triple nextInternal() throws IOException {
    String line = in.readLine();
    if (null == line) {
      return null;
    }
    return parser.parse(line);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
  
}
