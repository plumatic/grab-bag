package flop;
import java.util.Random;

public class DArray {
  public static int sampleDiscrete(Random r, double [] dist) {
      double p = r.nextDouble();
      for(int i = 0; i < dist.length; i++) {
          p -= dist[i];
          if (p <= 0) return i;
      }
      assert (dist.length > 0);
      return 0;
  }

  public static double logAdd(double[] logV) {
    double maxIndex = 0;
    double max = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < logV.length; i++) {
      if (logV[i] > max) {
        max = logV[i];
        maxIndex = i;
      }
    }
    if (max == Double.NEGATIVE_INFINITY) return Double.NEGATIVE_INFINITY;
    // compute the negative difference
    double threshold = max - DMath.LOGTOLERANCE;
    double sumNegativeDifferences = 0.0;
    for (int i = 0; i < logV.length; i++) {
      if (i != maxIndex && logV[i] > threshold) {
        sumNegativeDifferences += DMath.sloppyExpNegative(logV[i] - max);
      }
    }
    if (sumNegativeDifferences > 0.0) {
      return max + Math.log(1.0 + sumNegativeDifferences);
    } else {
      return max;
    }
  }
  

  public static void addInPlace(double[] accum, double[] by, double scale, double offset) {
    for (int i=0;i < accum.length; ++i) {
      double inc  = scale * by[i] + offset;
      if (inc != 0.0) accum[i] += inc;
    }
  }

  public static void multiplyInPlace(double[] accum, double[] by) {
    for (int i=0;i < accum.length; ++i) {
      accum[i] *= by[i];
    }
  }
}
