#ifndef _GHD_H_
#define _GHD_H_

struct GHDResult {
  uint64_t num_rows;
  void* result;
  GHDResult(uint64_t num_rows_in, void* result_in){
    num_rows = num_rows_in;
    result = result_in;
  }
  void get_result();
};

struct GHD {
	GHD();
	GHDResult* run();
};

#endif