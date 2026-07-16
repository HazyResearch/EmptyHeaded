#ifndef TIMER_H
#define TIMER_H

#include <iostream>
#include <chrono>

namespace timer{
  typedef std::chrono::steady_clock clock;
  typedef std::chrono::time_point<clock> time_point;

  static time_point start_clock (){
    return clock::now();
  }

  static inline double stop_clock(time_point t_in){
    std::chrono::duration<double> elapsed_seconds = clock::now()-t_in;
    return elapsed_seconds.count();
  }
  static double stop_clock(std::string in,time_point t_in){
    double t2= stop_clock(t_in);
    std::cout << "Time["+in+"]: " << t2 << " s" << std::endl;
    return t2;
  }
}
#endif
