package plumbing;

import clojure.java.api.Clojure;
import clojure.lang.ArraySeq;
import clojure.lang.BigInt;
import clojure.lang.IFn;
import clojure.lang.IMapEntry;
import clojure.lang.IPersistentList;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentSet;
import clojure.lang.IPersistentVector;
import clojure.lang.IRecord;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.LazilyPersistentVector;
import clojure.lang.PersistentList;
import clojure.lang.RT;
import clojure.lang.Seqable;
import clojure.lang.Var;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;

public class Serializer {
  private static final byte KEYWORD_TYPE =      0;
  private static final byte STRING_TYPE =       1;
  private static final byte INTEGER_TYPE =      2;
  private static final byte LONG_TYPE =         3;
  private static final byte BIG_INTEGER_TYPE =  4;
  private static final byte DOUBLE_TYPE =       5;
  private static final byte BOOLEAN_TYPE =      6;
  private static final byte CHAR_TYPE =         7;   // not yet implemented
  private static final byte NIL_TYPE =          8;
  private static final byte BINARY_TYPE =       9;   // not yet implemented
  private static final byte MAP_TYPE =          10;
  private static final byte VECTOR_TYPE =       11;
  private static final byte LIST_TYPE =         12;
  private static final byte SET_TYPE =          13;
  private static final byte FLOAT_TYPE =        14;
  private static final byte SERIALIZABLE_TYPE = 15;
  private static final byte BIG_INT_TYPE =      16;
  private static final byte RECORD_TYPE =       17;

  public static byte [] objectToBytes(Object o) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(os);
    oos.writeObject(o);
    oos.close();
    return os.toByteArray();
  }


  // this was a bad idea.
  //  static clojure.lang.Var readObject ;
  public static Object bytesToObject(byte [] bytes) throws Exception {
    //if (readObject == null) readObject = clojure.lang.RT.var("plumbing.serialize", "read-object-filthy-classloader-hack");
    //return readObject.invoke(new ObjectInputStream(new ByteArrayInputStream(bytes)));
    return (new ObjectInputStream(new ByteArrayInputStream(bytes))).readObject();
  }


  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private static void serializeMap(DataOutput dos, IPersistentMap obj) throws IOException {
    IPersistentMap map = (IPersistentMap) obj;
    dos.writeByte(MAP_TYPE);
    dos.writeInt(map.count());
    for (Object i : obj) {
      IMapEntry me = (IMapEntry) i;
      serialize(dos, me.key());
      serialize(dos, me.val());
    }
  }

  public static void serialize(DataOutput dos, Object obj) throws IOException {
    if (obj instanceof Keyword) {
      Keyword kw = (Keyword) obj;
      String ns = kw.getNamespace();
      byte [] nsbytes = ns!=null ? ns.getBytes(UTF_8) : null;
      int byteSize = ns!=null ? nsbytes.length + 1: 0;
      byte[] bytes = kw.getName().getBytes(UTF_8);
      byteSize += bytes.length;
      dos.writeByte(KEYWORD_TYPE);
      dos.writeInt(byteSize);
      if (ns != null) {
        dos.write(nsbytes, 0, nsbytes.length);
        dos.writeByte('/');
      }
      dos.write(bytes, 0, bytes.length);
    } else if (obj instanceof String) {
      String str = (String) obj;
      byte[] bytes = str.getBytes(UTF_8);
      int byteSize = bytes.length;
      dos.writeByte(STRING_TYPE);
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof Integer) {
      dos.writeByte(INTEGER_TYPE);
      dos.writeInt((Integer) obj);

    } else if (obj instanceof Long) {
      dos.writeByte(LONG_TYPE);
      dos.writeLong((Long) obj);

    } else if (obj instanceof BigInteger) {
      byte[] bytes = ((BigInteger) obj).toByteArray();
      int byteSize = bytes.length;
      dos.writeByte(BIG_INTEGER_TYPE);
      dos.writeInt(byteSize);
      dos.write(bytes, 0, byteSize);

    } else if (obj instanceof BigInt) {
      dos.writeByte(BIG_INT_TYPE);
      BigInt bi = (BigInt)obj;
      serialize(dos, bi.toBigInteger());

    } else if (obj instanceof Float) {
      dos.writeByte(FLOAT_TYPE);
      dos.writeFloat((Float) obj);

    } else if (obj instanceof Double) {
      dos.writeByte(DOUBLE_TYPE);
      dos.writeDouble((Double) obj);

    } else if (obj instanceof Boolean) {
      dos.writeByte(BOOLEAN_TYPE);
      dos.writeBoolean((Boolean) obj);

    } else if (obj == null) {
      dos.writeByte(NIL_TYPE);

    } else if (obj instanceof IRecord) {
      dos.writeByte(RECORD_TYPE);

      String recordFullName = obj.getClass().getCanonicalName();
      int lastDot = recordFullName.lastIndexOf('.');

      String pkg = "";
      if (0 <= lastDot) {
        pkg = recordFullName.substring(0, lastDot);
      }
      String recordClassName = recordFullName.substring(lastDot + 1);
      pkg = pkg.replace("_", "-");

      // verify lookup works
      lookupMapConstructor(pkg, recordClassName);

      /* write package and record class name */
      serialize(dos, pkg);
      serialize(dos, recordClassName);

      /* writes record as map */
      serializeMap(dos, (IPersistentMap) obj);
    } else if (obj instanceof IPersistentMap) {
      serializeMap(dos, (IPersistentMap) obj);
    } else if (obj instanceof IPersistentSet) {
      IPersistentSet set = (IPersistentSet) obj;
      ISeq sSeq = set.seq();

      dos.writeByte(SET_TYPE);
      dos.writeInt(set.count());
      while (sSeq != null) {
        serialize(dos, sSeq.first());
        sSeq = sSeq.next();
      }
    } else if (obj instanceof IPersistentVector) {
      IPersistentVector vec = (IPersistentVector) obj;
      int len = vec.count();
      dos.writeByte(VECTOR_TYPE);
      dos.writeInt(len);
      for (int i = 0; i < len; i++) {
        serialize(dos, vec.nth(i));
      }

    } else if ((obj instanceof IPersistentList) ||
        (obj instanceof ISeq)) {
      ISeq seq = ((Seqable) obj).seq();
      int len = (seq == null) ? 0 : seq.count();

      dos.writeByte(LIST_TYPE);
      dos.writeInt(len);

      while (seq != null) {
        serialize(dos, seq.first());
        seq = seq.next();
      }
    } else if (obj instanceof java.io.Serializable) {
      byte [] bytes = objectToBytes(obj);
      dos.writeByte(SERIALIZABLE_TYPE);
      dos.writeInt(bytes.length);
      dos.write(bytes);
    } else {
      throw new IOException("Cannot serialize " + obj);
    }
  }

  /* lookup the map->Record constructor method. Throw RuntimeException if it is unbound. */
  private static IFn lookupMapConstructor(String pkg, String recordClassName) {
    String recordCTor = "map->" + recordClassName;
    IFn v = Clojure.var(pkg, recordCTor);
    if (((Var)v).deref() instanceof Var.Unbound) {
      throw new RuntimeException(String.format("Error looking up map contructor for ns %s, record %s", pkg, recordCTor));
    }
    return v;
  }

  public static Object deserialize(DataInput dis) throws Exception {
    byte typeByte = dis.readByte();
    switch (typeByte) {
      case KEYWORD_TYPE:
        int keyByteSize = dis.readInt();
        byte[] keyBytes = new byte[keyByteSize];
        dis.readFully(keyBytes, 0, keyByteSize);
        String name = new String(keyBytes, UTF_8);
        Keyword keyword = Keyword.find(null,name);
        return (keyword != null ? keyword : Keyword.intern(name));

      case STRING_TYPE:
        int strByteSize = dis.readInt();
        byte[] strBytes = new byte[strByteSize];
        dis.readFully(strBytes, 0, strByteSize);
        return new String(strBytes, UTF_8);

      case INTEGER_TYPE:
        return dis.readInt();

      case LONG_TYPE:
        return dis.readLong();

      case BIG_INTEGER_TYPE:
        int byteSize = dis.readInt();
        byte[] bytes = new byte[byteSize];
        dis.readFully(bytes, 0, byteSize);
        return new BigInteger(bytes);

      case BIG_INT_TYPE:
        BigInteger bi = (BigInteger) deserialize(dis);
        return BigInt.fromBigInteger(bi);

      case FLOAT_TYPE:
        return dis.readFloat();

      case DOUBLE_TYPE:
        return dis.readDouble();

      case BOOLEAN_TYPE:
        return dis.readBoolean();

      case NIL_TYPE:
        return null;

      case MAP_TYPE:
        int mLen = dis.readInt() * 2;
        Object[] mObjs = new Object[mLen];
        for (int i = 0; i < mLen; i++) {
          mObjs[i] = deserialize(dis);
        }
        return RT.map(mObjs);

      case VECTOR_TYPE:
        int vLen = dis.readInt();
        Object[] vObjs = new Object[vLen];
        for (int i = 0; i < vLen; i++) {
          vObjs[i] = deserialize(dis);
        }
        return LazilyPersistentVector.createOwning(vObjs);

      case SET_TYPE:
        int  sLen = dis.readInt();
        Object[] sObjs = new Object[sLen];
        for (int i = 0; i < sLen; i++) {
          sObjs[i] = deserialize(dis);
        }
        return RT.set(sObjs);

      case LIST_TYPE:
        int lLen = dis.readInt();
        if (lLen == 0) {
          return PersistentList.EMPTY;
        } else {
          Object[] lObjs = new Object[lLen];
          for (int i = 0; i < lLen; i++) {
            lObjs[i] = deserialize(dis);
          }
          return ArraySeq.create(lObjs);
        }

      case RECORD_TYPE:
        String pkg = (String)deserialize(dis);
        String recordClassName = (String)deserialize(dis);
        Object recordMap = deserialize(dis);
        return lookupMapConstructor(pkg, recordClassName).invoke(recordMap);

      case SERIALIZABLE_TYPE:
        int bLen = dis.readInt();
        byte [] data = new byte[bLen];
        dis.readFully(data);
        return bytesToObject(data);

      default:
        throw new IOException("Cannot deserialize " + typeByte);
    }
  }
}
