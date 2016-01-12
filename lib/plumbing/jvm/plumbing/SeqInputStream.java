package plumbing;

import java.io.InputStream;
import clojure.lang.ISeq;

// Untested
public class SeqInputStream extends InputStream {
  public ISeq byteArraySeq;
  public int offset;
  
  private void advance() {
    while (byteArraySeq != null && offset == ((byte []) byteArraySeq.first()).length) {
      byteArraySeq = byteArraySeq.next();
      offset = 0;
    }
  }
  
  public SeqInputStream(ISeq byteArraySeq) {
    this.byteArraySeq = byteArraySeq;
    this.offset = 0;
    advance();
  }
  
  public int read() {
    if (byteArraySeq == null) return -1;
    byte [] bytes = (byte []) byteArraySeq.first();
    byte r = bytes[offset];
    offset++;
    advance();
    return r;
  }
  
  public int read(byte [] b) {
    return read(b, 0, b.length);
  }
  
  public int read(byte [] b, int off, int len) {
    if (len == 0) return 0;
    if (byteArraySeq == null) return -1;
    int c = 0;
    while (c < len && byteArraySeq != null) {
      byte [] bytes = (byte []) byteArraySeq.first();
      int n = Math.min(len-c, bytes.length - offset);
      System.arraycopy(bytes, offset, b, off + c, n);
      c += n;
      offset += n;
      advance();
    }
    return c;
  } 
}
