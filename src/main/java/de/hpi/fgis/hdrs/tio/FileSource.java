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

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import de.hpi.fgis.hdrs.parser.TripleParser;
import de.hpi.fgis.hdrs.parser.TripleParserFactory;

public class FileSource implements TripleSource, Closeable {
  
  private final File file;
  private final TripleParser<String> parser;
  
  public FileSource(File file, TripleParser<String> parser) {
    this.file = file;
    this.parser = parser;
  }
  
  public FileSource(File file) {
    this.file = file;
    this.parser = TripleParserFactory.getDefaultLineParser();
  }
  
  @Override
  public TripleScanner getScanner() throws IOException {
    return new StreamScanner(new FileReader(file), parser); 
  }
  
  @Override
  public void close() throws IOException {
    
  }
}
