package flop;

import java.util.Random;

// Adapted from https://github.com/percyliang/fig/blob/master/src/main/java/fig/prob/SampleUtils.java

// The MIT License (MIT)

// Copyright (c) 2006-2015 Percy Liang

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

public class Sample {

  // Return Gaussian(0, 1)
  public static double sampleGaussian(Random random) {
    // Use the Box-Muller Transformation
    // if x_1 and x_2 are independent uniform [0, 1],
    // then sqrt(-2 ln x_1) * cos(2*pi*x_2) is Gaussian with mean 0 and variance 1
    double x1 = random.nextDouble(), x2 = random.nextDouble();
    double z = Math.sqrt(-2*Math.log(x1))*Math.cos(2*Math.PI*x2);
    return z;
  }

  public static double sampleGamma(Random random, double a, double rate) {
    // G. Marsaglia and W.W. Tsang, A simple method for generating gamma
    // variables, ACM Transactions on Mathematical Software, Vol. 26, No. 3,
    // Pages 363-372, September, 2000.
    // http://portal.acm.org/citation.cfm?id=358414
    double boost;
    if(a < 1) {
      // boost using Marsaglia's (1961) method: gam(a) = gam(a+1)*U^(1/a)
      boost = Math.exp(Math.log(random.nextDouble())/a);
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
      if((u < 1-.0331*x*x) || (Math.log(u) < 0.5*x + d*(1-v+Math.log(v)))) {
        break;
      }
    }
    return boost*d*v / rate;
  }

  public static double sampleBeta(Random random, double alpha, double beta) {
    double a = sampleGamma(random, alpha, 1);
    double b = sampleGamma(random, beta, 1);
    return a/(a+b);
  }

}
