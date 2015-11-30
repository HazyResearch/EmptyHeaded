FFT Benchmarks
=======

In EMPTYHEADED_ROOT:

python ./runtime/codegenerator/fft/runGenFFT.py 4 8
cd storage_engine
make bin/fft_gen_4_8
./bin/fft_get_4_8

will create cpp, compile, and run an FFT of size 4^8 with base 4
It calculates the FFT of a single cosine wave and prints out
the first 5 components of the result. All terms should be 0 except
the second and the second to last (not printed).