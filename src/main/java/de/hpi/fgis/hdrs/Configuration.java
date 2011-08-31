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

package de.hpi.fgis.hdrs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;

import javax.imageio.stream.FileImageInputStream;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import de.hpi.fgis.hdrs.ipc.NodeProtocol;


public class Configuration extends org.apache.hadoop.conf.Configuration {

  /*
   *  Configuration keys and defaults
   */
  
  public static final String KEY_ROOT_DIR = "hdrs.rootdir";
  
  public static final int DEFAULT_RPC_PORT = 51337;
  
  public static final String KEY_NODE_TRANSACTION_BUFFER = "hdrs.node.transaction.buffer";
  public static final int DEFAULT_NODE_TRANSACTION_BUFFER = 16 * 1024 * 1024;
  public static final String KEY_RPC_HANDLER_COUNT = "hdrs.rpc.handler.count";
  public static final int DEFAULT_RPC_HANDLER_COUNT = 10;
  public static final String KEY_COMMIT_POOL_SIZE = "hdrs.commit.pool.size";
  public static final int DEFAULT_COMMIT_POOL_SIZE = 2;
  public static final String KEY_PREFTECH_POOL_SIZE = "hdrs.prefetch.pool.size";
  public static final int DEFAULT_PREFETCH_POOL_SIZE = 2;
  
  public static final String KEY_SCATTER_FACTOR = "hdrs.scatter.factor";
  public static final float DEFAULT_SCATTER_FACTOR = 3f;
  
  public static final String KEY_SEGMENT_BUFFER_SIZE = "hdrs.segment.buffer";
  public static final int DEFAULT_SEGMENT_BUFFER_SIZE = 32 * 1024 * 1024;
  public static final String KEY_SEGMENT_FLUSH_THRESHOLD = "hdrs.segment.flush.threshold";
  public static final int DEFAULT_SEGMENT_FLUSH_THRESHOLD = 24 * 1024 * 1024;
  public static final String KEY_SEGMENT_SPLIT_SIZE = "hdrs.segment.split.threshold";
  public static final int DEFAULT_SEGMENT_SPLIT_SIZE = 512 * 1024 * 1024;
  
  public static final String KEY_SEGMENT_MAX_FILES = "hdrs.segment.max.files";
  public static final int DEFAULT_SEGMENT_MAX_FILES = 10;
  public static final String KEY_SEGMENT_MIN_COMPACTION = "hdrs.segment.min.compaction";
  public static final int DEFAULT_SEGMENT_MIN_COMPACTION = 3;
  public static final String KEY_SEGMENT_MAX_COMPACTION = "hdrs.segment.max.compaction";
  public static final int DEFAULT_SEGMENT_MAX_COMPACTION = 6;
  public static final String KEY_SEGMENT_COMPACTION_RATIO = "hdrs.segment.compaction.ratio";
  public static final float DEFAULT_SEGMENT_COMPACTION_RATIO = 2f;
  public static final String KEY_SEGMENT_COMPACTION_THRESHOLD = "hdrs.segment.compaction.threshold";
  public static final int DEFAULT_SEGMENT_COMPACTION_THRESHOLD = 48 * 1024 * 1024;
  
  public static final String KEY_TRANSACTION_SIZE_NODE = "hdrs.transaction.size.node";
  public static final int DEFAULT_TRANSACTION_SIZE_NODE = 1 * 1024 * 1024;
  public static final String KEY_TRANSACTION_SIZE = "hdrs.transaction.size";
  public static final int DEFAULT_TRANSACTION_SIZE = 8 * DEFAULT_TRANSACTION_SIZE_NODE;
  
  private Configuration(boolean b) {
    super(b);
  }

  /**
   * Convert hadoop conf to hdrs conf (keep settings).
   */
  public Configuration(org.apache.hadoop.conf.Configuration conf) {
    super(conf);
  }
  
  public static Configuration create() {
    return create(1);
  }
  
  public static Configuration create(int nodeId) {
    // false = don't load hadoop defaults
    Configuration conf = new Configuration(false);
    conf.setInt("hdrs.local.node.id", nodeId);
    conf.addResource("hdrs-default.xml");
    conf.addResource("hdrs-site.xml");
    return conf;
  }
  
  public static ArrayList<Peer> readPeers(String file) 
  throws IOException {
    File f = new File(file);
    if (!f.isFile()) {
      throw new IOException("cannot find peer file");
    }
    FileImageInputStream in = new FileImageInputStream(f);
    String line = null;
    ArrayList<Peer> peers = new ArrayList<Peer>();
    while (null != (line = in.readLine())) {
      try {
        String[] splits = line.split("\t");
        int id = Integer.parseInt(splits[0]);
        int port = Integer.parseInt(splits[2]);
        peers.add(new Peer(id, splits[1], port));
      } catch (Throwable ex) {
        throw new IOException("error while reading peer file", ex);
      }
    }
    in.close();
    return peers;
  }
  
  public static Set<Triple.COLLATION> readIndexes(String file) 
  throws IOException {
    File f = new File(file);
    if (!f.isFile()) {
      throw new IOException("cannot find index file");
    }
    FileImageInputStream in = new FileImageInputStream(f);
    String line = null;
    Set<Triple.COLLATION> indexes = EnumSet.noneOf(Triple.COLLATION.class);
    while (null != (line = in.readLine())) {
      try {
        Triple.COLLATION index = Triple.COLLATION.valueOf(line);
        indexes.add(index);
      } catch (IllegalArgumentException ex) {
        throw new IOException("error readin index file: invalid index");
      }
    }
    in.close();
    return indexes;
  }
  
  
  public static StoreInfo loadStoreInfo(Configuration conf, String storeAddress) 
  throws IOException {
    if (0 > storeAddress.indexOf(':')) {
      storeAddress += ":" + Configuration.DEFAULT_RPC_PORT;
    }
    NodeProtocol proxy = Peer.getNodeProxy(conf, NetUtils.createSocketAddr(storeAddress));
    StoreInfo info = proxy.getStoreInfo();
    RPC.stopProxy(proxy);
    return info;
  }
  
  
  public static void main(String[] args) throws IOException {
    Configuration conf = Configuration.create();
    System.out.println(conf.toString());
    System.out.println(conf.get("hdrs.rootdir"));
    conf.writeXml(System.out);    
  }
  
}
