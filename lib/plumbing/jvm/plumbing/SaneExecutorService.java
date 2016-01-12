package plumbing;

import java.util.concurrent.*;
import java.util.Map;
import clojure.lang.AFn;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.IMeta;
import clojure.lang.Util;

public class SaneExecutorService extends ThreadPoolExecutor {
 
  
  public static class WeightedFutureTask extends FutureTask implements Comparable<WeightedFutureTask> {
    
    private AFn callback;
    public final double priority;
    

    
    @SuppressWarnings("unchecked")
    public WeightedFutureTask( AFn task){
      super(task);
      IPersistentMap meta = ((IMeta)task).meta();
      if(meta != null){
	this.callback = (AFn) meta.valAt(Keyword.intern("callback"));
	this.priority = (Double) meta.valAt(Keyword.intern("priority"),  Double.NaN);
      } else {
	this.priority = Double.NaN;
      }
    }
    
    protected void done() {
      try {
	if(! isCancelled() && callback != null) {
	  callback.invoke(this.get());
	}
      } catch (Throwable t){
	Util.sneakyThrow(t);
      }
      
    }
    
    public int compareTo(WeightedFutureTask other) {
      if(other.priority > priority){
	return 1;
      } else if(other.priority < priority){
	return -1;
      } else if(other.priority == priority){
	return 0;
      } else {
	throw new UnsupportedOperationException("attempted to compare when no comparison was valid:" + other.priority +", "+ priority);
      }
    }
    
    
  }

  @SuppressWarnings("unchecked")  
  public SaneExecutorService(int initThreads, int maxThreads, long keepAliveMs, BlockingQueue queue, RejectedExecutionHandler rejectHandler){
    super(initThreads, maxThreads, keepAliveMs, TimeUnit.MILLISECONDS, queue, Executors.defaultThreadFactory(), rejectHandler);    
  }
  
  @SuppressWarnings("unchecked")
  protected RunnableFuture newTaskFor(Callable task){
    return new WeightedFutureTask((AFn) task);
  }
  
  @SuppressWarnings("unchecked")
  protected RunnableFuture newTaskFor(Runnable task, Object value){
    throw new UnsupportedOperationException("please come back with Callable");
  }

  
}
