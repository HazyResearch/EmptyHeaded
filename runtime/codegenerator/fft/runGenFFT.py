import genFFT
import argparse

parser = argparse.ArgumentParser(description='Generate FFT c++ code.')
parser.add_argument(
    'base', type=int,
    help="Base used to encode inputs"
)
parser.add_argument(
    'power', type=int,
    help="Power used to encode inputs. N = base ^ power"
)


if __name__ == "__main__":
    args = parser.parse_args()

    gen = genFFT.GenFFT(args.base, args.power)
    print(gen.gen())

