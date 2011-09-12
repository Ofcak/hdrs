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

package de.hpi.fgis.hdrs.client;

import java.io.IOException;

import de.hpi.fgis.hdrs.Configuration;
import de.hpi.fgis.hdrs.Triple;
import de.hpi.fgis.hdrs.tio.TripleScanner;

public class Example {

  public static void main(String[] args) throws IOException {
    
    // connect to HDRS
    Client client = new Client(Configuration.create(), "address[:port]");
    
    //
    // READING / SCANNING
    //
    
    // full index scan of the SPO index
    TripleScanner scanner = client.getScanner(Triple.COLLATION.SPO);
       
    // alternatively: pattern scan on the SPO index
    //TripleScanner scanner = client.getScanner(Triple.COLLATION.SPO,
    //    Triple.newPattern("<http://sws.geonames.org/581166/>", null, null));    
        
    // read triples
    while (scanner.next()) {
      Triple t = scanner.pop();
      System.out.println(t);
    }
    
    // close the scanner to release server-side resources.
    scanner.close();
    
    //
    // WRITING
    //
    
    // get new triple output stream.  this stream writes to all indexes of the store.
    TripleOutputStream out = client.getOutputStream();
    
    out.add(Triple.newTriple("subject", "predicate", "object"));
    // ... write more triples ...
    
    // it is important to close the output stream.
    out.close();
    
    
    client.close();
  }

}
