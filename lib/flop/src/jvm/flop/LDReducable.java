package flop;

public interface LDReducable {
  // reduce over [idx val] values (fn accum idx val) 
  // starts with init
  public Object reduce(clojure.lang.IFn.OLDO fn, Object init);  
}
