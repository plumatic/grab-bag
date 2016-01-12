package hadoop_wrapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

/* Facilitates creation of an FSDataInputStream from an array of bytes.

   FSDataInputStream imposes requirements on its backing streams that
   aren't reflected in the constructor's type signature.  We needed a
   stream that could be constructed from a byte array and also
   implemented PositionedReadable and Seekable. As it turns out, there
   isn't one of those in the org.apache.hadoop.fs namespace, so some
   guy went ahead and rolled his own:
   SeekablePositionedReadableByteArrayInputStream. It's not complete,
   since he wasn't sure what exactly seekToNewSource should do, but it
   gets enough of the job done.

   The code is lifted verbatim from:
     http://www.javased.com/?source_dir=Timberwolf/src/test/java/com/ripariandata/timberwolf/writer/hive/SequenceFileMailWriterTest.java
*/

public class SeekablePositionedReadableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable 
{ 
  public SeekablePositionedReadableByteArrayInputStream(final byte[] data) 
  { 
    super(data); 
  } 

  // PositionReadable methods 

  public int read(final long position, final byte[] buffer, final int offset, final int length) throws IOException 
  { 
    this.mark(0); 

    int r = 0; 
    try 
    { 
      this.seek(position); 
      r = this.read(buffer, offset, length); 
    } 
    finally 
    { 
      this.reset(); 
    } 

    return r; 
  } 

  public void readFully(final long position, final byte[] buffer, final int offset, final int length) 
    throws IOException 
  { 
    int r = this.read(position, buffer, offset, length); 
    if (r != length) 
    { 
      throw new IOException(); 
    } 
  } 

  public void readFully(final long position, final byte[] buffer) throws IOException 
  { 
    this.readFully(position, buffer, 0, buffer.length); 
  } 

  // Seekable methods 

  public void seek(final long pos) throws IOException 
  { 
    if (pos > this.count) 
    { 
      throw new IOException(); 
    } 
    this.pos = (int) pos; 
  } 

  public long getPos() throws IOException 
  { 
    return this.pos; 
  } 

  public boolean seekToNewSource(final long targetPos) throws IOException 
  { 
    throw new IOException("I don't really know what this is supposed to do."); 
  } 
} 
