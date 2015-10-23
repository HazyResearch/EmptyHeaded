#ifndef TIMER_H
#define TIMER_H

#include <iostream>
#include <chrono>

namespace timer{
  static std::chrono::time_point<std::chrono::system_clock> start_clock (){
    return std::chrono::system_clock::now(); 
  }

  static inline double stop_clock(std::chrono::time_point<std::chrono::system_clock> t_in){
    std::chrono::duration<double> elapsed_seconds = std::chrono::system_clock::now()-t_in; 
    return elapsed_seconds.count();
  }
  static double stop_clock(std::string in,std::chrono::time_point<std::chrono::system_clock> t_in){
    double t2= stop_clock(t_in);
    std::cout << "Time["+in+"]: " << t2 << " s" << std::endl;
    return t2;
  }
}
#endif
