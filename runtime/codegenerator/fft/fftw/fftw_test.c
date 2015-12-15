#include <complex.h>
#include <math.H>
#include <fftw3.h>
#include <stdio.h>
#include <time.h>

/**
 * Benchmark fftw for comparison
 */
int main(int argc, char* argv[]) {
  fftw_complex *in, *out;
  fftw_plan p;

  int pow = 10;
  if (argc > 1) {
    sscanf(argv[1], "%d", &pow);
  }
  int N = 1 << pow;
  in = (fftw_complex*) fftw_malloc(sizeof(fftw_complex) * N);
  out = (fftw_complex*) fftw_malloc(sizeof(fftw_complex) * N);

  for (int i = 0; i < N; i++) {
    in[i] = cos((2.0*M_PI*i)/N) / N;
  }
  p = fftw_plan_dft_1d(N, in, out, FFTW_FORWARD, FFTW_MEASURE);

  clock_t t1, t2;
  for (int i = 0; i < 10; i++) {
    t1 = clock() / (CLOCKS_PER_SEC / 1000);
    fftw_execute(p); /* repeat as needed */
    t2 = clock() / (CLOCKS_PER_SEC / 1000);
    printf ("Took: %ld\n", t2 - t1);
  }

  for (int i=0; i<5; i++){
    printf("%f+%fi\n", creal(out[i]), cimag(out[i]));
  }

  fftw_destroy_plan(p);
  fftw_free(in); fftw_free(out);
}
