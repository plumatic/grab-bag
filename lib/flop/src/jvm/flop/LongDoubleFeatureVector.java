package flop;

import clojure.lang.Counted;
import java.io.*;
import java.util.*;
import gnu.trove.TLongDoubleHashMap;
import gnu.trove.TLongDoubleProcedure;
import gnu.trove.TLongIntHashMap;

public final class LongDoubleFeatureVector implements Counted, Iterable<Map.Entry<Long, Double> >, Externalizable, Cloneable, LDReducable {
  public final static long serialVersionUID = 7600069475578538731L;
  public final static double growthRate = 1.5;
  public long keys[];
  public double values[];
  public int count;    
  public TLongIntHashMap keyIndex;

  public LongDoubleFeatureVector(int initialCapacity) {
    count = 0;
    keys = new long[initialCapacity];
    values = new double[initialCapacity];
    keyIndex = new TLongIntHashMap();
  }
  
  public long[] keys() {
    return Arrays.copyOf(keys, size());
  }

  public LongDoubleFeatureVector() {
    this(4);
  }
  
  public LongDoubleFeatureVector(LongDoubleFeatureVector v) {
    this(v.size());
    putAll(v);
  }

  public LongDoubleFeatureVector(Map<Long, Double> m) {
    this(m.size());
    putAll(m);
  }

  public LongDoubleFeatureVector(TLongDoubleHashMap m) {
    this(m.size());
    putAll(m);
  }
  
  public LongDoubleFeatureVector clone() {
    return new LongDoubleFeatureVector(this);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    compact();
    out.writeByte(1);
    out.writeInt(count);
    out.writeObject(keys);
    out.writeObject(values);
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    if (in.readByte() != 1) throw new IOException("Unknown object version");
    count = in.readInt();
    keys = (long []) in.readObject();
    values = (double []) in.readObject();
    for(int i = 0; i < count; i++) keyIndex.put(keys[i], i);
  }

  public void clear() {
    count = 0;
    keys = new long[4];
    values = new double[4];
    keyIndex = new TLongIntHashMap();    
  }

  public int capacity() { 
    return keys.length; 
  }

  public int count() { 
    return count; 
  }

  public int size() { 
    return count;
  }

  public int indexOf(long k) {
    int i = keyIndex.get(k);
    if (i > 0) return i;
    if (count > 0 && keys[0] == k) return 0;
    return -1;
  }

  public double get(long k) {
    int i = indexOf(k);
    if (i < 0) return 0.0;
    return values[i];
  }

  private void rawPut(long k, double v) {
    int i = indexOf(k);
    if (i < 0) {
      if (v == 0.0) return;
      keys[count] = k;
      values[count] = v;
      keyIndex.put(k, count);
      count++;	
    } else {
      if (v == 0.0) {
	rawRemove(k);
      } else {
	values[i] = v;
      }
    }
  }

  private double rawIncrement(long k, double v) {
    int i = indexOf(k);
    if (i < 0) {
      keys[count] = k;
      values[count] = v;
      keyIndex.put(k, count);
      count++;	
      return v;
    } else {
      double newVal = values[i] + v;
      if (newVal == 0.0) {
	rawRemove(k);
      } else {
	values[i] += v;
      }
      return newVal;
    }
  }


  public void putAll(TLongDoubleHashMap m) {
    ensureCapacity(count + m.size());
    m.forEachEntry(new TLongDoubleProcedure() {
	public boolean execute(long a, double b) {
	  rawPut(a, b);
	  return true;
	}
      });
  }

  public void incrementAll(TLongDoubleHashMap m, final double scale) {
    ensureCapacity(count + m.size());
    m.forEachEntry(new TLongDoubleProcedure() {
	public boolean execute(long a, double b) {
	  rawIncrement(a, b * scale);
	  return true;
	}
      });
  }

  public void putAll(Map<Long, Double> m) {
    ensureCapacity(count + m.size());
    for(Map.Entry<Long, Double> e : m.entrySet()) {
      rawPut(e.getKey(), e.getValue());
    }
  }

  public void incrementAll(Map<Long, Double> m, final double scale) {
    ensureCapacity(count + m.size());
    for(Map.Entry<Long, Double> e : m.entrySet()) {
      rawIncrement(e.getKey(), e.getValue() * scale);
    }
  }
  
  public void putAll(LongDoubleFeatureVector m) {
    ensureCapacity(count + m.size());
    for(int i = 0; i < m.count; i++) {
      rawPut(m.keys[i], m.values[i]);
    }
  }

  public void incrementAll(LongDoubleFeatureVector m, final double scale) {
    ensureCapacity(count + m.size());
    for(int i = 0; i < m.count; i++) {
      rawIncrement(m.keys[i], m.values[i] * scale);
    }
  }


  public void put(long k, double v) {
    ensureCapacity(count + 1);
    rawPut(k, v);
  }

  public double increment(long k, double v) {
    ensureCapacity(count + 1);
    return rawIncrement(k, v);
  }

  public boolean rawRemove(long k) {
    int i = indexOf(k);
    if (i < 0) return false;
    keyIndex.remove(k);
    count--;

    if (count > i) {
      keys[i] = keys[count];
      values[i] = values[count];
      keyIndex.put(keys[i], i);
    }
    return true;
  }

  public boolean remove(long k) {
    if (!rawRemove(k)) return false;
    if (count < keys.length / (growthRate * growthRate)) resize((int) (count * growthRate)); 
    return true;
  }

  public boolean removeAll(List<Long> toRemove) {
    boolean modified = false;
    for (long l : toRemove) {
      modified = remove(l) || modified;
    }
    return modified;
  }

  public void ensureCapacity(int c) {
    if (c > keys.length) {
      resize((int) Math.max(c, (growthRate * keys.length)));
    }
  }

  public void resize(int c) {
    if (c < count) throw new IllegalArgumentException("cannot decrease capacity below size");
    keys = Arrays.copyOf(keys, c);
    values = Arrays.copyOf(values, c);
  }

  public void compact() {
    resize(count);
  }

  public class FVIterator implements Iterator<Map.Entry<Long, Double> > {
    int i;
    public FVIterator() {
      i = 0;
    }

    public boolean hasNext() {
      return i < count;
    }

    public Map.Entry<Long, Double> next() {
      return new java.util.AbstractMap.SimpleEntry<Long, Double>(keys[i], values[i++]);
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    public long key() {
      return keys[i];
    }
		
    public double value() {
      return values[i];
    }

  }

  public FVIterator fvIterator() {
    return new FVIterator();
  }

  public Iterator<Map.Entry<Long, Double> > iterator() {
    return new FVIterator();
  }

  public Map<Long, Double> asMap() {
    HashMap<Long, Double> m = new HashMap<Long, Double>();
    for(int i = 0; i < count; i++) {
      m.put(keys[i], values[i]);
    }
    return m;
  }
    
  public TLongDoubleHashMap asTrove() {
    TLongDoubleHashMap m = new TLongDoubleHashMap();
    for(int i = 0; i < count; i++) {
      m.put(keys[i], values[i]);
    }
    return m;
  }
  
  public double norm() {
    double norm = 0.0;
    for (int i = 0; i < count; i++) {
      norm += values[i] * values[i];
    }
    return Math.sqrt(norm);
  }

  public double diffNorm(LongDoubleFeatureVector o) {
    double norm = 0.0;
    for (int i = 0; i < count; i++) {
      double d = values[i] - o.get(keys[i]);
      norm += d * d;
    }
    for (int i = 0; i < o.count; i++) {
      if (indexOf(o.keys[i]) <= 0) {
	double d = 0 - o.values[i];
	norm += d * d;
      }
    }
    return Math.sqrt(norm);
  }
  
  public LongDoubleFeatureVector scaleInPlace(double scale) {
    for (int i = 0; i < count; i++) {
      values[i] *= scale;
    }
    return this;
  }


  public LongDoubleFeatureVector normalizeInPlace() {
    double norm = norm();
    if (norm > 0.0) {
      scaleInPlace(1 / norm);
    }
    return this;
  }
  
  public LongDoubleFeatureVector normalized() {
    return new LongDoubleFeatureVector(this).normalizeInPlace();
  }


  public double dotProduct(TLongDoubleHashMap m) {
    double r = 0.0;
    if (m.size() > 0) {
      for(int i = 0 ; i < count; i++) {
	r += values[i] * m.get(keys[i]);	
      }
    }
    return r;
  }

  public double dotProduct(LongDoubleFeatureVector m) {
    double r = 0.0;    
    for(int i = 0 ; i < count; i++) {
      r += values[i] * m.get(keys[i]);	
    }
    return r;
  }
  

  public static double dotProduct(final TLongDoubleHashMap m1, final TLongDoubleHashMap m2) {
    if (m1.size() > m2.size()) {
      return dotProduct(m2, m1);
    }
    double r = 0.0;	
    gnu.trove.TLongDoubleIterator it = m1.iterator();
    while(it.hasNext()) {
      it.advance();
      r += it.value() * m2.get(it.key());
    }
    return r;
  }

  public double dotProduct(double[] ds) {
    double r = 0.0;
    if (ds.length > 0) {
      for(int i = 0 ; i < count; i++) {
	r += values[i] * ds[(int)keys[i]];
      }
    }
    return r;
  }

  public double dotProduct(float[] ds) {
    double r = 0.0;
    if (ds.length > 0) {
      for(int i = 0 ; i < count; i++) {
	r += values[i] * ds[(int)keys[i]];
      }
    }
    return r;
  }

  public void forEachEntry(clojure.lang.IFn.LDO f) {
    int initCount = count;
    for(int i = 0; i < count; i++) {
      f.invokePrim(keys[i], values[i]);
    }
    if (count != initCount) {
      throw new RuntimeException("You cannot add or remove items in a LongDoubleFeatureVector while iterating over it");	
    }
  }
  
  public Object reduce(clojure.lang.IFn.OLDO f, Object init) {
    for(int i = 0; i < count; i++) {
      init = f.invokePrim(init, keys[i], values[i]);
    }
    return init;
  }


  public LongDoubleFeatureVector mapKeys(clojure.lang.IFn.LDL f) {
    LongDoubleFeatureVector fv = new LongDoubleFeatureVector(this.count);
    for (int i=0; i < count; ++i) {
      long newKey = f.invokePrim(keys[i], values[i]);
      fv.put(newKey, values[i]);
    }
    return fv;
  }

  public LongDoubleFeatureVector mapVals(clojure.lang.IFn.LDD f) {
    LongDoubleFeatureVector fv = new LongDoubleFeatureVector(this.count);
    for (int i=0; i < count; ++i) {
      double newVal = f.invokePrim(keys[i], values[i]);
      fv.put(keys[i], newVal);
    }
    return fv;
  }
}
