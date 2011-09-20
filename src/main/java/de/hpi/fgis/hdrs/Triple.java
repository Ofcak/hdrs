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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import de.hpi.fgis.hdrs.ipc.Writable;

/**
 * <p> A triple.  This triple implementation uses one contiguous byte buffer
 * to store subject, predicate, and object - one after each other.
 * A triple instance holds a pointer to the backing buffer; an offset into
 * the buffer where the subject starts; the length of subject, predicate,
 * and object; and the multiplicity.
 * 
 * <p> This storage format is closely related to how triples are persisted on
 * disk (see TripleFileWriter).  This enable zero-copy when reading triples
 * from disk into memory.
 * 
 * <p> This class also contains the implementation of Collation orders (SPO,
 * POS, etc.) which are implemented through a number of different comparators.
 * 
 * @author hefenbrock
 *
 */
public class Triple implements Writable {

  static final Log LOG = LogFactory.getLog(Triple.class);

  protected byte[] buffer;
  protected int offset;
  
  protected short Slen;
  protected short Plen;
  protected int Olen;
  
  protected int multiplicity;
  
  static int ensureNonMagic(int multiplicity) {
    if(Integer.MIN_VALUE == multiplicity) {
      LOG.warn("Ensuring non-magic triple multiplicity!");
      return Integer.MIN_VALUE + 1;
    }
    return multiplicity;
  }

  /**
   * Construct a new triple.
   * @param buf The byte buffer holding subject, predicate, and object
   * @param offset The offset into buf where subject starts
   * @param slen The length of subject in bytes
   * @param plen The length of predicate in bytes
   * @param olen The length of object in bytes
   * @param multiplicity The multiplicity of the triple
   * @return A triple object
   */
  public static Triple newInstance(final byte[] buf, int offset, short slen, short plen, 
      int olen, int multiplicity) {
    return new Triple(buf, offset, slen, plen, olen, ensureNonMagic(multiplicity));
  }
  
  //for Writable
  private Triple() {
    offset = 0;
  } 
  
  private Triple(final byte[] buf, int offset, short slen, short plen, int olen,
      int multiplicity) {
    buffer = buf;
    this.offset = offset;
    Slen = slen;
    Plen = plen;
    Olen = olen;
    this.multiplicity = multiplicity;
  }
  
  
  /**
   * Returns a reference to the byte array backing this triple.
   */
  public byte[] getBuffer() {
    return buffer;
  }
  
  /**
   * @return Offset into the backing byte array where subject starts.
   */
  public int getOffset() {
    return offset;
  }
  
  /**
   * @return Length in bytes of subject.
   */
  public short getSubjectLength() {
    return Slen;
  }
  
  /**
   * @return Length in bytes of predicate.
   */
  public short getPredicateLength() {
    return Plen;
  }
  
  /**
   * @return Length in bytes of object.
   */
  public int getObjectLength() {
    return Olen;
  }
  
  
  private void setSubjectLength(short len) {
    Slen = len;
  }
  
  private void setPredicateLength(short len) {
    Plen = len;
  }
  
  private void setObjectLength(int len) {
    Olen = len;
  }
  
  
  /**
   * @return Offset into the backing byte array where predicate starts.
   */
  public int getPredicateOffset() {
    return offset + (Slen < 0 ? 0 : Slen);
  }
  
  /**
   * @return Offset into the backing byte array where object starts.
   */
  public int getObjectOffset() {
    return getPredicateOffset() + (int) (Plen < 0 ? 0 : Plen);
  }
  
  
  /**
   * <p> The multiplicity denotes how many times a triple is represented.
   * Note that also negative multiplicities are possible.
   * A multiplicity of N is interpreted as follows:
   * <ul><li> N > 0:  The triple is present exactly N times.
   * <li> N == 0:  The triple is not present.  Used to encode patterns.
   * <li> N < 0:  The triple is deleted N times.
   * </ul>
   * @return Multiplicity of this triple.
   */
  public int getMultiplicity() {
    return multiplicity;
  }
  
  
  /**
   * @return True if this instance represents a pattern.
   */
  public boolean isPattern() {
    return 0 == multiplicity;
  }
  
  
  /**
   * @return True if this instance represents a (or multiple) delete(s).
   */
  public boolean isDelete() {
    return 0 > multiplicity;
  }
  
  
  /**
   * Copy this triple.  The copy will have a private buffer
   * tailored to hold subject, predicate and object.
   * @return Copy of this Triple.
   */
  public Triple copy() {
    byte[] buf = new byte[bufferSize()];
    System.arraycopy(buffer, offset, buf, 0, bufferSize());
    return new Triple(buf, 0, Slen, Plen, Olen, multiplicity);
  }
  
  
  @Override
  public Triple clone() {
    return new Triple(buffer, offset, Slen, Plen, Olen, multiplicity);
  }
  
  
  /**
   * Adds up the multiplicity of two identical triples.
   * Note this method does not allocate a new triple, it rather changes
   * the multiplicity of t1 which is then returned.
   * @param t1 A triple
   * @param t2 A triple equal to t1
   * @return A triple equal to t1 and t2, having a multiplicity of
   * multiplicity(t1) + multiplicity(t2), or null, if the multiplicity
   * adds to zero.
   */
  public static Triple merge(Triple t1, Triple t2) {
    int m = t1.multiplicity + t2.multiplicity;
    if (0 == m) {
      return null;
    }
    t1.multiplicity = ensureNonMagic(m);
    return t1;
  }
  
  
  public void merge(Triple otherTriple) {
    this.multiplicity = ensureNonMagic(multiplicity + otherTriple.multiplicity);
  }
  
  
  /**
   * @return A delete for this triple (multiplicity. = -multiplicity).
   */
  public Triple getDelete() {
    Triple del = copy();
    // FIXME check: one might throw a UnsupportedOperationException int the case of a magic triple?
    if(isMagic()) {
      throw new UnsupportedOperationException("Unable to getDelete() of a magic triple \"" + del + "\"!");
    }
    // alternative:
    del.multiplicity = -multiplicity;
    return del;
  }
  
  
  /**
   * @return Total size in bytes of Subject, Predicate, and Object. 
   * No headers included!
   */
  public int bufferSize() {
    return (int) (Slen < 0 ? 0 : Slen) 
        + (int) (Plen < 0 ? 0 : Plen)
        + (Olen < 0 ? 0 : Olen);
  }
  
  /**
   * Estimate total IN MEMORY size of this triple.  Including headers and data.
   * This estimate is for 32bit.
   * @return  Estimated size of this triple in bytes.
   */
  public int estimateSize() {
    return (9* Integer.SIZE) / Byte.SIZE + bufferSize();
  }
  
  public static final int HEADER_SIZE = (3* Integer.SIZE) / Byte.SIZE;
  
  /**
   * @return Size of this triple when serialized.  Including headers and data.
   */
  public int serializedSize() {
    return HEADER_SIZE + bufferSize();
  }
  
  
  // For testing / debugging only (slow)
  
  private Triple(String subject, String predicate, String object, int multiplicity) {
    this(subject.concat(predicate).concat(object).getBytes(), 
        0, (short) subject.length(), (short) predicate.length(),
        object.length(), multiplicity);
  }
  
  /**
   * Create a new triple.  This method should only be used for testing as it does
   * expensive copying of the input strings provided.
   * @param subject The subject string.
   * @param predicate The predicate string.
   * @param object The object string.
   * @param multiplicity The multiplicity.
   * @return A new triple
   */
  public static Triple newTriple(String subject, String predicate, String object, 
      int multiplicity) {
    return new Triple(subject, predicate, object, ensureNonMagic(multiplicity));
  }
  
  public static Triple newTriple(String subject, String predicate, String object) {
    return new Triple(subject, predicate, object, (short) 1);
  }
  
  /**
   * <p> Create a pattern from string.  This method should only be used for testing 
   * as it does expensive copying of the input strings provided.
   * <p> A wild card (*) is denoted by passing null as subject, predicate, and/or
   * object.
   * @param subject The predicate string or null.
   * @param predicate The object string or null.
   * @param object The object The object string or null.
   * @return  A new pattern.
   */
  public static Triple newPattern(String subject, String predicate, String object) {
    return newInstance(subject, predicate, object, 0);
  }
  
  public static Triple newDelete(String subject, String predicate, String object) {
    return newInstance(subject, predicate, object, -1);
  }
  
  /**
   * Create a pattern.  Pass null for wild cards (*)
   */
  private static Triple newInstance(String subject, String predicate, String object,
      int multiplicity) {
    String buf = "";
    int slen, plen, olen;
    if (null != subject) {
      buf = buf.concat(subject);
      slen = subject.length();
    } else {
      slen = -1;
    }
    if (null != predicate) {
      buf = buf.concat(predicate);
      plen = predicate.length();
    } else {
      plen = -1;
    }
    if (null != object) {
      buf = buf.concat(object);
      olen = object.length();
    } else {
      olen = -1;
    }
    return new Triple(buf.getBytes(), 0, (short) slen, (short) plen, olen, multiplicity);
  }
  
  /**
   * This is slow as it involves creating a byte array copy.
   * @return A string representing subject.
   */
  public String getSubject() {
    if (0 > Slen) {
      return null;
    }
    return new String(buffer, offset, Slen);
  }
  
  /**
   * This is slow as it involves creating a byte array copy.
   * @return A string representing predicate.
   */
  public String getPredicate() {
    if (0 > Plen) {
      return null;
    }
    return new String(buffer, getPredicateOffset(), (int) Plen);
  }
  
  /**
   * This is slow as it involves creating a byte array copy.
   * @return A string representing object.
   */
  public String getObject() {
    if (0 > Olen) {
      return null;
    }
    return new String(buffer, getObjectOffset(), Olen);
  }
  
  @Override
  public String toString() {
    return "(" + getSubject() + ", " 
                + getPredicate() + ", "
                + getObject() + ", "
                + getMultiplicity() + ")";
  }
  
  
  public Triple clip(Triple.COLLATION order, int len) {
    if (bufferSize() <= len) {
      return this;
    }
    Triple t = clone();
    int clip = t.bufferSize() - len;
    
    order.setPos3Len(t, Math.max(0, order.getPos3Len(t) - clip));
    clip -= order.getPos3Len(this);
    
    if (0 < clip) {
      order.setPos2Len(t, Math.max(0, order.getPos2Len(t) - clip));
      clip -= order.getPos2Len(this);
    }
    if (0 < clip) {
      order.setPos1Len(t, Math.max(0, order.getPos1Len(t) - clip));
    }
    t.multiplicity = Integer.MIN_VALUE; // make it magic
    return t;
  }
  
  
  /*
   *  Write format for triples.  It is designed to be used as on-disk format
   *  as well as in-memory format.  The idea is that we can map an entire 
   *  block into memory and create triple views on it, without ever copying 
   *  any memory.
   *  
   *  -----------------------------------------------------------------------------
   *  | SLength | PLength | OLength | Multiplicity | Subject | Predicate | Object |
   *  |---------|---------|---------|--------------|---------|-----------|--------| 
   *  | 2 byte  | 2 byte  | 4 byte  | 4 byte       |  var.   | var.      | var.   |
   *  -----------------------------------------------------------------------------
   *  
   */ 
  
  
  public static void writeTriple(DataOutput stream, Triple t) throws IOException {
    stream.writeShort(t.getSubjectLength());
    stream.writeShort(t.getPredicateLength());
    stream.writeInt(t.getObjectLength());
    stream.writeInt(t.getMultiplicity());
    if (0 < t.getSubjectLength())
      stream.write(t.getBuffer(), t.getOffset(), t.getSubjectLength());
    if (0 < t.getPredicateLength())
      stream.write(t.getBuffer(), t.getPredicateOffset(), t.getPredicateLength());
    if (0 < t.getObjectLength())
      stream.write(t.getBuffer(), t.getObjectOffset(), t.getObjectLength());
  }
  
  public static void writeTripleHeader(ByteBuffer out, Triple t) {
    out.putShort(t.getSubjectLength());
    out.putShort(t.getPredicateLength());
    out.putInt(t.getObjectLength());
    out.putInt(t.getMultiplicity());
  }
  
  public static void writeTripleData(ByteBuffer out, Triple t) {
    if (0 < t.getSubjectLength())
      out.put(t.getBuffer(), t.getOffset(), t.getSubjectLength());
    if (0 < t.getPredicateLength())
      out.put(t.getBuffer(), t.getPredicateOffset(), t.getPredicateLength());
    if (0 < t.getObjectLength())
      out.put(t.getBuffer(), t.getObjectOffset(), t.getObjectLength());
  }
  
  public static void writeTriple(ByteBuffer buffer, Triple t) {
    buffer.putShort(t.getSubjectLength());
    buffer.putShort(t.getPredicateLength());
    buffer.putInt(t.getObjectLength());
    buffer.putInt(t.getMultiplicity());
    if (0 < t.getSubjectLength())
      buffer.put(t.getBuffer(), t.getOffset(), t.getSubjectLength());
    if (0 < t.getPredicateLength())
      buffer.put(t.getBuffer(), t.getPredicateOffset(), t.getPredicateLength());
    if (0 < t.getObjectLength())
      buffer.put(t.getBuffer(), t.getObjectOffset(), t.getObjectLength());
  }
  
  public static Triple readTriple(ByteBuffer header, ByteBuffer data) {
    Triple triple = new Triple();
    triple.readFields(header, data);
    return triple;
  }
  
  public static Triple readTriple(ByteBuffer buffer) {
    Triple triple = new Triple();
    triple.readFields(buffer);
    return triple;
  }
  
  public void readFields(ByteBuffer header, ByteBuffer data) {
    // read header
    Slen = header.getShort();
    Plen = header.getShort();
    Olen = header.getInt();
    multiplicity = header.getInt();
    // read data
    this.buffer = data.array();
    int size = (int) Slen + (int) Plen + Olen;
    offset = data.arrayOffset() + data.position();
    data.position(data.position() + size);
  }
  
  public void readFields(ByteBuffer buffer) {
    // read header
    Slen = buffer.getShort();
    Plen = buffer.getShort();
    Olen = buffer.getInt();
    multiplicity = buffer.getInt();
    // read data
    this.buffer = buffer.array();
    int size = (int) Slen + (int) Plen + Olen;
    offset = buffer.arrayOffset() + buffer.position();
    buffer.position(buffer.position() + size);
  }
  
  public static Triple readTriple(DataInput in) throws IOException {
    Triple triple = new Triple();
    triple.readFields(in);
    return triple;
  }
  
  //
  // following implements a special "magic triple"
  //
  
  public static final Triple MAGIC_TRIPLE = new Triple(
      new byte[]{}, 0, (short) 0, (short) 0, 0, Integer.MIN_VALUE);
  
  public boolean isMagic() {
    return multiplicity == Integer.MIN_VALUE;
  }
  
  
  //
  //  Writable -- used for RPCs
  //
    
  @Override
  public void readFields(DataInput in) throws IOException {
    // read header
    Slen = in.readShort();
    Plen = in.readShort();
    Olen = in.readInt();
    multiplicity = in.readInt();
    // read data
    //int size = Slen + (int) Plen + Olen;
    int size = bufferSize();
    buffer = new byte[size];
    in.readFully(buffer, 0, size);
  }


  @Override
  public void write(DataOutput out) throws IOException {
    writeTriple(out, this);
  }
  
  
  /**
   * This enumeration defines collation orders (SPO, POS, etc.).  It can
   * be used to retrieve comparators for all orders.
   * @author hefenbrock
   *
   */
  public static enum COLLATION {
    
    SPO ((byte) 1) { 
      @Override Comparator createComparator() { return new SPOComparator(); }
      @Override int getPos1Len(Triple t) { return t.getSubjectLength(); }
      @Override void setPos1Len(Triple t, int len) { t.setSubjectLength((short) len); }
      @Override int getPos1Off(Triple t) { return t.getOffset(); }
      @Override int getPos2Len(Triple t) { return t.getPredicateLength(); }
      @Override void setPos2Len(Triple t, int len) { t.setPredicateLength((short) len); }
      @Override int getPos2Off(Triple t) { return t.getPredicateOffset(); }
      @Override int getPos3Len(Triple t) { return t.getObjectLength(); }
      @Override void setPos3Len(Triple t, int len) { t.setObjectLength(len); }
      @Override int getPos3Off(Triple t) { return t.getObjectOffset(); }
      @Override int readPos3Len(ByteBuffer in) { return in.getInt(); }
      @Override void writePos3Len(Triple t, ByteBuffer out) { 
        out.putInt(t.getObjectLength()); }
      @Override
      Triple newInstance(byte[] buf, int off, int len, int off2, int len2,
          int off3, int len3, int multiplicity) {
        return Triple.newInstance(buf, off, (short) len, off2, (short) len2, off3, len3, 
            multiplicity);
      }},
    SOP ((byte) 2) { 
        @Override Comparator createComparator() { return new SOPComparator(); }
        @Override int getPos1Len(Triple t) { return t.getSubjectLength(); }
        @Override void setPos1Len(Triple t, int len) { t.setSubjectLength((short) len); }
        @Override int getPos1Off(Triple t) { return t.getOffset(); }
        @Override int getPos2Len(Triple t) { return t.getObjectLength(); }
        @Override void setPos2Len(Triple t, int len) { t.setObjectLength(len); }
        @Override int getPos2Off(Triple t) { return t.getObjectOffset(); }
        @Override int getPos3Len(Triple t) { return t.getPredicateLength(); }
        @Override void setPos3Len(Triple t, int len) { t.setPredicateLength((short) len); }
        @Override int getPos3Off(Triple t) { return t.getPredicateOffset(); }
        
        @Override int readPos2Len(ByteBuffer in) { return in.getInt(); }
        @Override void writePos2Len(Triple t, ByteBuffer out) { 
          out.putInt(t.getObjectLength()); }
        @Override
        Triple newInstance(byte[] buf, int off, int len, int off2, int len2,
            int off3, int len3, int multiplicity) {
          return Triple.newInstance(buf, off, (short) len, off3, (short) len3, off2, len2, 
              multiplicity);
      }},
    POS ((byte) 3) { 
        @Override Comparator createComparator() { return new POSComparator(); }
        @Override int getPos1Len(Triple t) { return t.getPredicateLength(); }
        @Override void setPos1Len(Triple t, int len) { t.setPredicateLength((short) len); }
        @Override int getPos1Off(Triple t) { return t.getPredicateOffset(); }
        @Override int getPos2Len(Triple t) { return t.getObjectLength(); }
        @Override void setPos2Len(Triple t, int len) { t.setObjectLength(len); }
        @Override int getPos2Off(Triple t) { return t.getObjectOffset(); }
        @Override int getPos3Len(Triple t) { return t.getSubjectLength(); }
        @Override void setPos3Len(Triple t, int len) { t.setSubjectLength((short) len); }
        @Override int getPos3Off(Triple t) { return t.getOffset(); }
        @Override int readPos2Len(ByteBuffer in) { return in.getInt(); }
        @Override void writePos2Len(Triple t, ByteBuffer out) { 
          out.putInt(t.getObjectLength()); }
        @Override
        Triple newInstance(byte[] buf, int off, int len, int off2, int len2,
            int off3, int len3, int multiplicity) {
          return Triple.newInstance(buf, off3, (short) len3, off, (short) len, off2, len2, 
              multiplicity);
      }},
    PSO ((byte) 4) { 
        @Override Comparator createComparator() { return new PSOComparator(); }
        @Override int getPos1Len(Triple t) { return t.getPredicateLength(); }
        @Override void setPos1Len(Triple t, int len) { t.setPredicateLength((short) len); }
        @Override int getPos1Off(Triple t) { return t.getPredicateOffset(); }
        @Override int getPos2Len(Triple t) { return t.getSubjectLength(); }
        @Override void setPos2Len(Triple t, int len) { t.setSubjectLength((short) len); }
        @Override int getPos2Off(Triple t) { return t.getOffset(); }
        @Override int getPos3Len(Triple t) { return t.getObjectLength(); }
        @Override void setPos3Len(Triple t, int len) { t.setObjectLength(len); }
        @Override int getPos3Off(Triple t) { return t.getObjectOffset(); }
        @Override int readPos3Len(ByteBuffer in) { return in.getInt(); }
        @Override void writePos3Len(Triple t, ByteBuffer out) { 
          out.putInt(t.getObjectLength()); }
        @Override
        Triple newInstance(byte[] buf, int off, int len, int off2, int len2,
            int off3, int len3, int multiplicity) {
          return Triple.newInstance(buf, off2, (short) len2, off, (short) len, off3, len3, 
              multiplicity);
      }},
    OSP ((byte) 5) { 
        @Override Comparator createComparator() { return new OSPComparator(); }
        @Override int getPos1Len(Triple t) { return t.getObjectLength(); }
        @Override void setPos1Len(Triple t, int len) { t.setObjectLength(len); }
        @Override int getPos1Off(Triple t) { return t.getObjectOffset(); }
        @Override int getPos2Len(Triple t) { return t.getSubjectLength(); }
        @Override void setPos2Len(Triple t, int len) { t.setSubjectLength((short) len); }
        @Override int getPos2Off(Triple t) { return t.getOffset(); }
        @Override int getPos3Len(Triple t) { return t.getPredicateLength(); }
        @Override void setPos3Len(Triple t, int len) { t.setPredicateLength((short) len); }
        @Override int getPos3Off(Triple t) { return t.getPredicateOffset(); }
        @Override int readPos1Len(ByteBuffer in) { return in.getInt(); }
        @Override void writePos1Len(Triple t, ByteBuffer out) { 
          out.putInt(t.getObjectLength()); }
        @Override
        Triple newInstance(byte[] buf, int off, int len, int off2, int len2,
            int off3, int len3, int multiplicity) {
          return Triple.newInstance(buf, off2, (short) len2, off3, (short) len3, off, len, 
              multiplicity);
      }},
    OPS ((byte) 6) { 
        @Override Comparator createComparator() { return new OPSComparator(); }
        @Override int getPos1Len(Triple t) { return t.getObjectLength(); }
        @Override void setPos1Len(Triple t, int len) { t.setObjectLength(len); }
        @Override int getPos1Off(Triple t) { return t.getObjectOffset(); }
        @Override int getPos2Len(Triple t) { return t.getPredicateLength(); }
        @Override void setPos2Len(Triple t, int len) { t.setPredicateLength((short) len); }
        @Override int getPos2Off(Triple t) { return t.getPredicateOffset(); }
        @Override int getPos3Len(Triple t) { return t.getSubjectLength(); }
        @Override void setPos3Len(Triple t, int len) { t.setSubjectLength((short) len); }
        @Override int getPos3Off(Triple t) { return t.getOffset(); }
        @Override int readPos1Len(ByteBuffer in) { return in.getInt(); }
        @Override void writePos1Len(Triple t, ByteBuffer out) { 
          out.putInt(t.getObjectLength()); }
        @Override
        Triple newInstance(byte[] buf, int off, int len, int off2, int len2,
            int off3, int len3, int multiplicity) {
          return Triple.newInstance(buf, off3, (short) len3, off2, (short) len2, off, len, 
              multiplicity);
      }};
    
    abstract Comparator createComparator();
    
    public Comparator comparator() {
      if (null == comparator) {
        comparator = createComparator();
      }
      return comparator;
    }
    
    public MagicComparator magicComparator() {
      if (null == magicComparator) {
        magicComparator = new MagicComparator(comparator());
      }
      return magicComparator;
    }
    
    private final byte code;
    private Comparator comparator = null;
    private MagicComparator magicComparator = null;
    
    COLLATION(final byte c) {
      code = c;
    }
    
    public byte getCode() {
      return code;
    }
    
    public static COLLATION decode(final byte code) {
      if (SPO.code == code) return SPO;
      if (SOP.code == code) return SOP;
      if (POS.code == code) return POS;
      if (PSO.code == code) return PSO;
      if (OSP.code == code) return OSP;
      if (OPS.code == code) return OPS;
      throw new IllegalArgumentException("Invalid collation encoding");
    }
    
    public static Set<Triple.COLLATION> allOf(Triple.COLLATION...orders) {
      Set<Triple.COLLATION> res = EnumSet.noneOf(Triple.COLLATION.class);
      for (int i=0; i<orders.length; ++i) {
        res.add(orders[i]);
      }
      return res;
    }
    
    public static Triple.COLLATION parse(String str) {
      return Triple.COLLATION.valueOf(str.toUpperCase());
    }
    
    abstract int getPos1Off(Triple t);
    abstract int getPos2Off(Triple t);
    abstract int getPos3Off(Triple t);
    
    abstract int getPos1Len(Triple t);
    abstract int getPos2Len(Triple t);
    abstract int getPos3Len(Triple t);
    
    abstract void setPos1Len(Triple t, int len);
    abstract void setPos2Len(Triple t, int len);
    abstract void setPos3Len(Triple t, int len);
    
    void writePos1Len(Triple t, ByteBuffer out) { out.putShort((short) getPos1Len(t)); }
    void writePos2Len(Triple t, ByteBuffer out) { out.putShort((short) getPos2Len(t)); }
    void writePos3Len(Triple t, ByteBuffer out) { out.putShort((short) getPos3Len(t)); }
    
    int readPos1Len(ByteBuffer in) { return in.getShort(); }
    int readPos2Len(ByteBuffer in) { return in.getShort(); }
    int readPos3Len(ByteBuffer in) { return in.getShort(); }
    
    abstract Triple newInstance(byte[] buf, int p1Off, int p1Len, int p2Off, int p2Len, 
        int p3Off, int p3Len, int multiplicity);
    
    public Triple mask(Triple t, boolean maskPos2) {
      return newInstance(t.getBuffer(), 
          getPos1Off(t), getPos1Len(t),
          getPos2Off(t), maskPos2 ? -1 : getPos2Len(t),
          getPos2Off(t), -1, 0);
    }
  }
  
  
  /**
   * A comparator for comparing triples using a defined collation order
   * (SPO, POS, etc.)
   * @author hefenbrock
   *
   */
  public static abstract class Comparator implements java.util.Comparator<Triple> {
    
    /**
     * @return The collation order implemented by this comparator.
     */
    public abstract COLLATION getCollation();
    
    /**
     * Match triples.  Matching takes wild cards into account: wild cards
     * match everything, including other wild cards.  For example, in SPO,
     * (A, B, *) matches (A, B, C).  Note that matching is symmetric.
     * @param t1
     * @param t2
     * @return True, if t1 and t2 match.
     */
    public abstract boolean match(Triple t1, Triple t2);
    
    /**
     * Lexicographically compare two byte arrays.  This is taken from
     * hbase.util.Bytes
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @param offset1 Where to start comparing in the left buffer
     * @param offset2 Where to start comparing in the right buffer
     * @param length1 How much to compare from the left buffer
     * @param length2 How much to compare from the right buffer
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public static int compareBuffers(
        byte[] buffer1, int offset1, int length1,
        byte[] buffer2, int offset2, int length2) {
      // Bring WritableComparator code local
      int end1 = offset1 + length1;
      int end2 = offset2 + length2;
      for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
        int a = (buffer1[i] & 0xff);
        int b = (buffer2[j] & 0xff);
        if (a != b) {
          return a - b;
        }
      }
      return length1 - length2;
    }
    
    public static int compare(
        byte[] buffer1, int offset1, int length1,
        byte[] buffer2, int offset2, int length2) {
      if (length1 >= 0 && length2 >= 0) {
        return compareBuffers(buffer1, offset1, length1,
            buffer2, offset2, length2);
      }
      if (length1 < 0) {
        return length2 < 0 ? 0 : -1;
      }
      return 1;
    }
    
    public static boolean match(
        byte[] buffer1, int offset1, int length1,
        byte[] buffer2, int offset2, int length2) {
      if (length1 >= 0 && length2 >= 0) {
        return buffersEqual(buffer1, offset1, length1,
            buffer2, offset2, length2);
      }
      // one or both are wild cards
      return true;
    }
    
  }
  
  
  public static class MagicComparator implements java.util.Comparator<Triple> {

    final private Comparator baseComparator;
    
    MagicComparator(Comparator base) {
      baseComparator = base;
    }
    
    public COLLATION getCollation() {
      return baseComparator.getCollation();
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      if (t1.isMagic()) {
        return t2.isMagic() ? 0 : -1;
      }
      if (t2.isMagic()) {
        return 1;
      }
      return baseComparator.compare(t1, t2);
    }
    
    public Comparator getBase() {
      return baseComparator;
    }
    
  }
  
  
  static class SPOComparator extends Comparator {

    @Override
    public COLLATION getCollation() {
      return COLLATION.SPO;
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      int cmp = compare(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (0 != cmp) return cmp;
      cmp = compare(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
                      t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (0 != cmp) return cmp;
      return compare(t1.buffer, t1.getObjectOffset(), t1.Olen,
                      t2.buffer, t2.getObjectOffset(), t2.Olen);
    }
    
    @Override
    public boolean match(Triple t1, Triple t2) {
      boolean match = match(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (!match) return false;
      match = match(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (!match) return false;
      return match(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
    }
    
  }
  
  
  static class SOPComparator extends Comparator {

    @Override
    public COLLATION getCollation() {
      return COLLATION.SOP;
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      int cmp = compare(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (0 != cmp) return cmp;
      cmp = compare(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (0 != cmp) return cmp;
      return compare(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
    }
    
    @Override
    public boolean match(Triple t1, Triple t2) {
      boolean match = match(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (!match) return false;
      match = match(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (!match) return false;
      return match(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
    }
    
  }
  
  
  static class POSComparator extends Comparator {

    @Override
    public COLLATION getCollation() {
      return COLLATION.POS;
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      int cmp = compare(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (0 != cmp) return cmp;
      cmp = compare(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (0 != cmp) return cmp;
      return compare(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
    }
    
    @Override
    public boolean match(Triple t1, Triple t2) {
      boolean match = match(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
            t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (!match) return false;
      match = match(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (!match) return false;
      return match(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
    }
    
  }
  
  
  static class PSOComparator extends Comparator {

    @Override
    public COLLATION getCollation() {
      return COLLATION.PSO;
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      int cmp = compare(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (0 != cmp) return cmp;
      cmp = compare(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (0 != cmp) return cmp;
      return compare(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
    }
    
    @Override
    public boolean match(Triple t1, Triple t2) {
      boolean match = match(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
            t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (!match) return false;
      match = match(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (!match) return false;
      return match(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
    }
    
  }
  
  
  static class OSPComparator extends Comparator {

    @Override
    public COLLATION getCollation() {
      return COLLATION.OSP;
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      int cmp = compare(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (0 != cmp) return cmp;
      cmp = compare(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (0 != cmp) return cmp;
      return compare(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
    }
    
    @Override
    public boolean match(Triple t1, Triple t2) {
      boolean match = match(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (!match) return false;
      match = match(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
      if (!match) return false;
      return match(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
    }
    
  }
  
  
  static class OPSComparator extends Comparator {

    @Override
    public COLLATION getCollation() {
      return COLLATION.OPS;
    }

    @Override
    public int compare(Triple t1, Triple t2) {
      int cmp = compare(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (0 != cmp) return cmp;
      cmp = compare(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (0 != cmp) return cmp;
      return compare(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
    }
    
    @Override
    public boolean match(Triple t1, Triple t2) {
      boolean match = match(t1.buffer, t1.getObjectOffset(), t1.Olen,
          t2.buffer, t2.getObjectOffset(), t2.Olen);
      if (!match) return false;
      match = match(t1.buffer, t1.getPredicateOffset(), (int) t1.Plen,
          t2.buffer, t2.getPredicateOffset(), (int) t2.Plen);
      if (!match) return false;
      return match(t1.buffer, t1.offset, t1.Slen,
          t2.buffer, t2.offset, t2.Slen);
    }
    
  }
  
  
  // from http://mindprod.com/jgloss/hashcode.html
  /*public static int hashByteBuffer(byte[] buf, int off, int len) {
    int hash = 0;
    for (int i=off; i<off+len; ++i) {
      // rotate left and xor (very fast in assembler, a bit clumsy to specify 
      // in Java, but a smart compiler might collapse it to two machine instructions)
      hash <<= 1;
      if (hash < 0) {
        hash |= 1;
      }
      hash ^= buf[i];
    }
    return hash;
  }*/
  
  
  public static int hashByteBuffer(byte[] buf, int off, int len) {
    return JenkinsHash.hash(buf, off, len, 0);
  }
  
  
  @Override
  public int hashCode() {
    // e^(log((2^31) - 1) / 3) = 22.4354722 ---> 23
    int result = hashByteBuffer(buffer, offset, 0 > Slen ? 0 : Slen);
    result = 23 * result + hashByteBuffer(
        buffer, getPredicateOffset(), 0 > Plen ? 0 : Plen);
    result = 23 * result + hashByteBuffer(
        buffer, getObjectOffset(), 0 > Olen ? 0 : Olen);
    return result;
  }
  
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Triple)) {
      return false;
    }
    Triple other = (Triple) obj;
    return buffersEqual(getBuffer(), getOffset(), getSubjectLength(),
        other.getBuffer(), other.getOffset(), other.getSubjectLength())
        && buffersEqual(getBuffer(), getPredicateOffset(), getPredicateLength(),
            other.getBuffer(), other.getPredicateOffset(), other.getPredicateLength())
        && buffersEqual(getBuffer(), getObjectOffset(), getObjectLength(),
            other.getBuffer(), other.getObjectOffset(), other.getObjectLength());
  }
  
  
  static Triple newInstance(final byte[] buf, int offset, short slen, int pOffset, 
      short plen, int oOffset, int olen, int multiplicity) {
    return new ScatteredTriple(buf, offset, slen, pOffset, plen, oOffset, olen, multiplicity);
  }
  
  
  public static boolean buffersEqual(
      byte[] buffer1, int offset1, int length1,
      byte[] buffer2, int offset2, int length2) {
    if (length1 >= 0 && length2 >= 0) {
      if (length1 != length2) {
        return false;
      }
      int end1 = offset1 + length1;
      int end2 = offset2 + length2;
      for (int i = end1-1, j = end2-1; i >= offset1 && j >= offset2; i--, j--) {
        int a = (buffer1[i] & 0xff);
        int b = (buffer2[j] & 0xff);
        if (a != b) {
          return false;
        }
      }
      return true;
    }
    return length1 < 0 && length2 < 0; 
  }
  
  static class ScatteredTriple extends Triple {
    
    private int predicateOffset;
    private int objectOffset;
    
    ScatteredTriple() {}
    
    private ScatteredTriple(final byte[] buf, int offset, short slen, int pOffset, short plen, 
        int oOffset, int olen, int multiplicity) {
      super(buf, offset, slen, plen, olen, multiplicity);
      predicateOffset = pOffset;
      objectOffset = oOffset;
    }
    
    public int getPredicateOffset() {
      return predicateOffset;
    }
    
    public int getObjectOffset() {
      return objectOffset;
    }
    
    public int estimateSize() {
      return (2* Integer.SIZE) / Byte.SIZE + super.estimateSize();
    }
    
    public Triple copy() {
      byte[] buf = new byte[bufferSize()];
      System.arraycopy(buffer, offset, buf, 0, Slen);
      System.arraycopy(buffer, predicateOffset, buf, Slen, Plen);
      System.arraycopy(buffer, objectOffset, buf, Slen+Plen, Olen);
      return new Triple(buf, 0, Slen, Plen, Olen, multiplicity);
    }
    
    @Override
    public Triple clone() {
      return new ScatteredTriple(buffer, offset, Slen, predicateOffset, 
          Plen, objectOffset, Olen, multiplicity);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      predicateOffset = super.getPredicateOffset();
      objectOffset = super.getObjectOffset();
    }
    
  }
  
}
