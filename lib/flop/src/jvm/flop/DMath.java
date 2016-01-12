package flop;
import java.util.Random;

public class DMath {
  
  // Tuck these in here until we go 1.3
  public static long leftShift(long l, int i) { return l << i; }
  public static long rightShift(long l, int i) { return l >> i; }
  public static long unsignedRightShift(long l, int i) { return l >>> i; }
  public static int intCast(long l) { return (int) l; }

  static final double LOGTOLERANCE = 30.0;

  // Return Gaussian(0, 1)
  public static double sampleGaussian(Random random) {
    // Use the Box-Muller Transformation
    // if x_1 and x_2 are independent uniform [0, 1],
    // then sqrt(-2 ln x_1) * cos(2*pi*x_2) is Gaussian with mean 0 and variance 1
    double x1 = random.nextDouble(), x2 = random.nextDouble();
    double z = Math.sqrt(-2*sloppyLog(x1))*Math.cos(2*Math.PI*x2);
    return z;
  }

  public static double sampleStudentT(Random random, long dof) {
    // Use a Box-Muller Transformation
    // http://www.ams.org/journals/mcom/1994-62-206/S0025-5718-1994-1219702-8/S0025-5718-1994-1219702-8.pdf
    while(true) {
      double u = 2 * random.nextDouble() - 1, v = 2 * random.nextDouble() - 1;
      double w = u * u + v * v;
      if (w <= 1) { 
        double r2 = dof * (Math.pow(w, -2.0 / dof) - 1);
        return u * Math.sqrt(r2 / w);
      }
    }
  }
  
  public static double sampleGamma(Random random, double a, double rate) {
    // G. Marsaglia and W.W. Tsang, A simple method for generating gamma
    // variables, ACM Transactions on Mathematical Software, Vol. 26, No. 3,
    // Pages 363-372, September, 2000.
    // http://portal.acm.org/citation.cfm?id=358414
    double boost;
    if(a < 1) {
      // boost using Marsaglia's (1961) method: gam(a) = gam(a+1)*U^(1/a)
      boost = sloppyExp(sloppyLog(random.nextDouble())/a);
      ++a;
    } 
    else {
      boost = 1;
    }

    double d = a-1.0/3, c = 1.0/Math.sqrt(9*d);
    double v;
    while(true) {
      double x;
      do {
        x = sampleGaussian(random);
        v = 1+c*x;
      } while(v <= 0);
      v = v*v*v;
      x = x*x;
      double u = random.nextDouble();
      if((u < 1-.0331*x*x) || (sloppyLog(u) < 0.5*x + d*(1-v+sloppyLog(v)))) {
        break;
      }
    }
    return boost*d*v / rate;
  }

  // stolen from Radford Neal
  public static double digamma(double x) {
    double r = 0.0;
    
    while (x <= 5) {
      r -= 1 / x;
      x += 1;
    }
    
    double f = 1.0 / (x * x);
    double t = f * (-1 / 12.0 + 
		    f * (1 / 120.0 + 
		    f * (-1 / 252.0 + 
                    f * (1 / 240.0 + 
                    f * (-1 / 132.0 + 
                    f * (691 / 32760.0 + 
                    f * (-1 / 12.0 + 
                    f * 3617.0 / 8160.0)))))));
   return r + sloppyLog(x) - 0.5 / x + t;
 }

  //stolen from http://introcs.cs.princeton.edu/91float/Gamma.java.html
  public static double logGamma(double x) {
    double tmp = (x - 0.5) * sloppyLog(x + 4.5) - (x + 4.5);
    double ser = 1.0 + 76.18009173    / (x + 0)   - 86.50532033    / (x + 1)
                     + 24.01409822    / (x + 2)   -  1.231739516   / (x + 3)
                     +  0.00120858003 / (x + 4)   -  0.00000536382 / (x + 5);
    return tmp + sloppyLog(ser * Math.sqrt(2 * Math.PI));
  }  

  
  public static double sloppyExp(double x) {
    // if x is very near one, use the linear approximation
    if (x < -LOGTOLERANCE) return 0.0;
    if (Math.abs(x) < 0.001) return 1 + x;
    return Math.exp(x);
  }
  
  private static double[] sloppyExpCache;
  private final static int sloppyExpBins = 100000;
  private final static double invBinSize = sloppyExpBins / LOGTOLERANCE;
  public static double sloppyExpNegative(double x) {
    assert (x <= 0);
    if (sloppyExpCache == null) {
      sloppyExpCache = new double [sloppyExpBins + 1];
      double halfbin = LOGTOLERANCE/(sloppyExpBins*2);
      for(int i = 0; i < sloppyExpBins; i++) {
	sloppyExpCache[i] = Math.exp((i * LOGTOLERANCE / sloppyExpBins) - LOGTOLERANCE + halfbin);
      }
      sloppyExpCache[sloppyExpBins] = 1.0;
    }
    if (x <= -LOGTOLERANCE) return 0.0;
    double i = ((x + LOGTOLERANCE) * invBinSize);
    return sloppyExpCache[(int) i];    
  }

  public static double[] cachedLogs;
  public static double MIN_LOG = -LOGTOLERANCE;
  public static double MAX_LOG = 600;
  public static int NUM_APPROX = 20000;

  public static double sloppyLog(double x) {
    return Math.log(x);
  }

}
