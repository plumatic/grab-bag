package flop;

public interface IWeightVector extends LDReducable {
  // Full possible dimension
  public long dimension();
  // # of active dimensions
  public long active_dimension();
  // value at position idx
  public double val_at(long idx);
  // increment value at position idx
  public void inc_BANG_(long idx, double val);
  // how to do dot-product against caonical dense
  public double dot_product(double[] dense);
  // how to do dot-product against caonical sparse
  public double dot_product(LongDoubleFeatureVector other);
  public Object to_data();
}

