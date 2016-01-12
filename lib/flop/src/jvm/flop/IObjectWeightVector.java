package flop;

// A Weight Vector that also knows how to index Objects.
public interface IObjectWeightVector extends IWeightVector {
  public double val_at(Object o); 
  public double dot_product(java.util.Collection other);
  public LongDoubleFeatureVector index(java.util.Collection other);
  public Object reduce(clojure.lang.IFn.OODO fn, Object init);
}
