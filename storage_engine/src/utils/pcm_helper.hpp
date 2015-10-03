#ifndef _PCM_HELPER_HPP_
#define _PCM_HELPER_HPP_

#include "common.hpp"

#define ENABLE_PCM

#include <iostream>
#include <math.h>
#include <iomanip>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <cstring>
#include <sstream>
#include <assert.h>
#include "cpucounters.h"
#include "utils.h"

#define READ 0
#define WRITE 1
#define PARTIAL 2

#define MAX_SOCKETS 256
#define MAX_IMC_CHANNELS 8

template <class IntType>
double float_format(IntType n)
{
    return double(n) / 1024 / 1024;
}

std::string temp_format(int32 t)
{
    char buffer[1024];
    if (t == PCM_INVALID_THERMAL_HEADROOM)
        return "N/A";

    sprintf(buffer, "%2d", t);
    return buffer;
}

std::string l3cache_occ_format(uint64 o)
{
    char buffer[1024];
    if (o == PCM_INVALID_L3_CACHE_OCCUPANCY)
        return "N/A";

    sprintf(buffer, "%6d", (uint32) o);
    return buffer;
}

struct raiders_pcm {
   std::vector<CoreCounterState> cstates1, cstates2;
   std::vector<SocketCounterState> sktstate1, sktstate2;
   SystemCounterState sstate1, sstate2;
   int cpu_model;
   uint64 TimeAfterSleep = 0;
   ServerUncorePowerState * BeforeState;
   ServerUncorePowerState * AfterState;
   uint64 BeforeTime = 0, AfterTime = 0;


   uint64 BRANCH_INSTR_RETIRED_events = 0;
   uint64 BRANCH_MISSES_RETIRED_events = 0;

   PCM::CustomCoreEventDescription MyEvents[4];

   PCM* m;
   raiders_pcm(){
      std::cout<< "PCM Init..." << std::endl;
      m = PCM::getInstance();
      cpu_model = m->getCPUModel();
      m->disableJKTWorkaround();
      //int32_t ret_val = 0;

      BeforeState = new ServerUncorePowerState[m->getNumSockets()];
      AfterState = new ServerUncorePowerState[m->getNumSockets()];

      switch(m->program()) {
         case PCM::Success:
            std::cout << "PCM initialized" << std::endl;
            break;
         case PCM::PMUBusy:
            m->resetPMU();
            break;
         default:
            break;
      }
      //return ret_val;
   }
   ~raiders_pcm(){ 
      m->resetPMU();
      //return 0;
   }

   void init_counter_states() {
      m->getAllCounterStates(sstate1, sktstate1, cstates1);


      for(uint32 i=0; i<m->getNumSockets(); ++i)
         BeforeState[i] = m->getServerUncorePowerState(i); 

      BeforeTime = m->getTickCount();


      //Page 220 of the link below
      //http://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-software-developers-manual.pdf


      MyEvents[0].event_number = 0x88; // architectural "branch instruction retired" event number

      MyEvents[0].umask_value = 0x01; // architectural "branch instruction retired" event umask

      MyEvents[1].event_number = 0x89; // architectural "branch misses retired event" number

      MyEvents[1].umask_value = 0x01; // architectural "branch misses retired" event umask

   }
   void end_counter_states(){
      
      m->getAllCounterStates(sstate2, sktstate2, cstates2);

      AfterTime = m->getTickCount();
      for(uint32 i=0; i<m->getNumSockets(); ++i)
         AfterState[i] = m->getServerUncorePowerState(i);

      BRANCH_INSTR_RETIRED_events = getNumberOfCustomEvents(0,sstate1, sstate2); 
      BRANCH_MISSES_RETIRED_events = getNumberOfCustomEvents(1,sstate1, sstate2);
   }

   //copied from pcm.cpp
   void print_output(PCM * m,
       const std::vector<CoreCounterState> & cstates1,
       const std::vector<CoreCounterState> & cstates2,
       const std::vector<SocketCounterState> & sktstate1,
       const std::vector<SocketCounterState> & sktstate2,
       const SystemCounterState& sstate1,
       const SystemCounterState& sstate2,
       const int cpu_model,
       const bool show_core_output,
       const bool show_socket_output,
       const bool show_system_output
       )
   {
       std::cout << "\n";
       std::cout << " EXEC  : instructions per nominal CPU cycle" << "\n";
       std::cout << " IPC   : instructions per CPU cycle" << "\n";
       std::cout << " FREQ  : relation to nominal CPU frequency='unhalted clock ticks'/'invariant timer ticks' (includes Intel Turbo Boost)" << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " AFREQ : relation to nominal CPU frequency while in active state (not in power-saving C state)='unhalted clock ticks'/'invariant timer ticks while in C0-state'  (includes Intel Turbo Boost)" << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " L3MISS: L3 cache misses " << "\n";
       if (cpu_model == PCM::ATOM)
           std::cout << " L2MISS: L2 cache misses " << "\n";
       else
           std::cout << " L2MISS: L2 cache misses (including other core's L2 cache *hits*) " << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " L3HIT : L3 cache hit ratio (0.00-1.00)" << "\n";
       std::cout << " L2HIT : L2 cache hit ratio (0.00-1.00)" << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " L3CLK : ratio of CPU cycles lost due to L3 cache misses (0.00-1.00), in some cases could be >1.0 due to a higher memory latency" << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " L2CLK : ratio of CPU cycles lost due to missing L2 cache but still hitting L3 cache (0.00-1.00)" << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " READ  : bytes read from memory controller (in GBytes)" << "\n";
       if (cpu_model != PCM::ATOM) std::cout << " WRITE : bytes written to memory controller (in GBytes)" << "\n";
       if (m->memoryIOTrafficMetricAvailable()) std::cout << " IO    : bytes read/written due to IO requests to memory controller (in GBytes); this may be an over estimate due to same-cache-line partial requests" << "\n";
       if (m->L3CacheOccupancyMetricAvailable()) std::cout << " L3OCC : L3 occupancy (in KBytes)" << "\n";
       std::cout << " TEMP  : Temperature reading in 1 degree Celsius relative to the TjMax temperature (thermal headroom): 0 corresponds to the max temperature" << "\n";
       std::cout << "\n";
       std::cout << "\n";
       std::cout.precision(2);
       std::cout << std::fixed;
       if (cpu_model == PCM::ATOM)
           std::cout << " Core (SKT) | EXEC | IPC  | FREQ | L2MISS | L2HIT | TEMP" << "\n" << "\n";
       else
       {
            std::cout << " Core (SKT) | EXEC | IPC  | FREQ  | AFREQ | L3MISS | L2MISS | L3HIT | L2HIT | L3CLK | L2CLK |";

            if (m->L3CacheOccupancyMetricAvailable())
                  std::cout << "  L3OCC | READ | WRITE |";
            else
                  std::cout << "  READ | WRITE |";
            
            if (m->memoryIOTrafficMetricAvailable())
                  std::cout << "  IO   | TEMP |" << "\n" << "\n";
               else
                  std::cout << " TEMP" << "\n" << "\n";
            
       }


       if (show_core_output)
       {
           for (uint32 i = 0; i < m->getNumCores(); ++i)
           {
               if (m->isCoreOnline(i) == false)
                   continue;

               if (cpu_model != PCM::ATOM)
               {
                   std::cout << " " << std::setw(3) << i << "   " << std::setw(2) << m->getSocketId(i) <<
                       "     " << getExecUsage(cstates1[i], cstates2[i]) <<
                       "   " << getIPC(cstates1[i], cstates2[i]) <<
                       "   " << getRelativeFrequency(cstates1[i], cstates2[i]) <<
                       "    " << getActiveRelativeFrequency(cstates1[i], cstates2[i]) <<
                       "    " << unit_format(getL3CacheMisses(cstates1[i], cstates2[i])) <<
                       "   " << unit_format(getL2CacheMisses(cstates1[i], cstates2[i])) <<
                       "    " << getL3CacheHitRatio(cstates1[i], cstates2[i]) <<
                       "    " << getL2CacheHitRatio(cstates1[i], cstates2[i]) <<
                       "    " << getCyclesLostDueL3CacheMisses(cstates1[i], cstates2[i]) <<
                       "    " << getCyclesLostDueL2CacheMisses(cstates1[i], cstates2[i]) ;
                   if (m->L3CacheOccupancyMetricAvailable())
                       std::cout << "   " << std::setw(6) << l3cache_occ_format(getL3CacheOccupancy(cstates2[i])) ;
                   if (m->memoryIOTrafficMetricAvailable())
                       std::cout << "     N/A     N/A     N/A";
                   else
                       std::cout << "     N/A     N/A";
                   std::cout << "     " << temp_format(cstates2[i].getThermalHeadroom()) <<
                       "\n";
               }
               else
                   std::cout << " " << std::setw(3) << i << "   " << std::setw(2) << m->getSocketId(i) <<
                   "     " << getExecUsage(cstates1[i], cstates2[i]) <<
                   "   " << getIPC(cstates1[i], cstates2[i]) <<
                   "   " << getRelativeFrequency(cstates1[i], cstates2[i]) <<
                   "   " << unit_format(getL2CacheMisses(cstates1[i], cstates2[i])) <<
                   "    " << getL2CacheHitRatio(cstates1[i], cstates2[i]) <<
                   "     " << temp_format(cstates2[i].getThermalHeadroom()) <<
                   "\n";
           }
       }
       if (show_socket_output)
       {
           if (!(m->getNumSockets() == 1 && cpu_model == PCM::ATOM))
           {
               std::cout << "-----------------------------------------------------------------------------------------------------------------------------" << "\n";
               for (uint32 i = 0; i < m->getNumSockets(); ++i)
               {
                   std::cout << " SKT   " << std::setw(2) << i <<
                       "     " << getExecUsage(sktstate1[i], sktstate2[i]) <<
                       "   " << getIPC(sktstate1[i], sktstate2[i]) <<
                       "   " << getRelativeFrequency(sktstate1[i], sktstate2[i]) <<
                       "    " << getActiveRelativeFrequency(sktstate1[i], sktstate2[i]) <<
                       "    " << unit_format(getL3CacheMisses(sktstate1[i], sktstate2[i])) <<
                       "   " << unit_format(getL2CacheMisses(sktstate1[i], sktstate2[i])) <<
                       "    " << getL3CacheHitRatio(sktstate1[i], sktstate2[i]) <<
                       "    " << getL2CacheHitRatio(sktstate1[i], sktstate2[i]) <<
                       "    " << getCyclesLostDueL3CacheMisses(sktstate1[i], sktstate2[i]) <<
                       "    " << getCyclesLostDueL2CacheMisses(sktstate1[i], sktstate2[i]);
                   if (m->L3CacheOccupancyMetricAvailable())
                       std::cout << "    " << std::setw(6) << l3cache_occ_format(getL3CacheOccupancy(sktstate2[i])) ;
                   if (m->memoryTrafficMetricsAvailable())
                       std::cout << "    " << getBytesReadFromMC(sktstate1[i], sktstate2[i]) / double(1024ULL * 1024ULL * 1024ULL) <<
                       "    " << getBytesWrittenToMC(sktstate1[i], sktstate2[i]) / double(1024ULL * 1024ULL * 1024ULL);
                   else
                          std::cout << " N/A     N/A";

                   if (m->memoryIOTrafficMetricAvailable())
                       std::cout << "    " << getIORequestBytesFromMC(sktstate1[i], sktstate2[i]) / double(1024ULL * 1024ULL * 1024ULL);

                   std::cout << "     " << temp_format(sktstate2[i].getThermalHeadroom()) << "\n";
               }
           }
       }
       std::cout << "-----------------------------------------------------------------------------------------------------------------------------" << "\n";

       if (show_system_output)
       {
           if (cpu_model != PCM::ATOM)
           {
               std::cout << " TOTAL  *     " << getExecUsage(sstate1, sstate2) <<
                   "   " << getIPC(sstate1, sstate2) <<
                   "   " << getRelativeFrequency(sstate1, sstate2) <<
                   "    " << getActiveRelativeFrequency(sstate1, sstate2) <<
                   "    " << unit_format(getL3CacheMisses(sstate1, sstate2)) <<
                   "   " << unit_format(getL2CacheMisses(sstate1, sstate2)) <<
                   "    " << getL3CacheHitRatio(sstate1, sstate2) <<
                   "    " << getL2CacheHitRatio(sstate1, sstate2) <<
                   "    " << getCyclesLostDueL3CacheMisses(sstate1, sstate2) <<
                   "    " << getCyclesLostDueL2CacheMisses(sstate1, sstate2) ;
               if (m->L3CacheOccupancyMetricAvailable())
                   std::cout << "    " << " N/A ";
               if (m->memoryTrafficMetricsAvailable())
                   std::cout << "    " << getBytesReadFromMC(sstate1, sstate2) / double(1024ULL * 1024ULL * 1024ULL) <<
                   "    " << getBytesWrittenToMC(sstate1, sstate2) / double(1024ULL * 1024ULL * 1024ULL);
               else
                   std::cout << "     N/A     N/A";

               if (m->memoryIOTrafficMetricAvailable())
                   std::cout << "    " << getIORequestBytesFromMC(sstate1, sstate2) / double(1024ULL * 1024ULL * 1024ULL);

               std::cout << "     N/A\n";
           }
           else
               std::cout << " TOTAL  *     " << getExecUsage(sstate1, sstate2) <<
               "   " << getIPC(sstate1, sstate2) <<
               "   " << getRelativeFrequency(sstate1, sstate2) <<
               "   " << unit_format(getL2CacheMisses(sstate1, sstate2)) <<
               "    " << getL2CacheHitRatio(sstate1, sstate2) <<
               "     N/A\n";
       }

       if (show_system_output)
       {
           std::cout << "\n" << " Instructions retired: " << unit_format(getInstructionsRetired(sstate1, sstate2)) << " ; Active cycles: " << unit_format(getCycles(sstate1, sstate2)) << " ; Time (TSC): " << unit_format(getInvariantTSC(cstates1[0], cstates2[0])) << "ticks ; C0 (active,non-halted) core residency: " << (getCoreCStateResidency(0, sstate1, sstate2)*100.) << " %\n";
           std::cout << "\n";
           for (int s = 1; s <= PCM::MAX_C_STATE; ++s)
           if (m->isCoreCStateResidencySupported(s))
               std::cout << " C" << s << " core residency: " << (getCoreCStateResidency(s, sstate1, sstate2)*100.) << " %;";
           std::cout << "\n";
           for (int s = 0; s <= PCM::MAX_C_STATE; ++s)
           if (m->isPackageCStateResidencySupported(s))
               std::cout << " C" << s << " package residency: " << (getPackageCStateResidency(s, sstate1, sstate2)*100.) << " %;";
           std::cout << "\n";
           if (m->getNumCores() == m->getNumOnlineCores())
           {
               std::cout << "\n" << " PHYSICAL CORE IPC                 : " << getCoreIPC(sstate1, sstate2) << " => corresponds to " << 100. * (getCoreIPC(sstate1, sstate2) / double(m->getMaxIPC())) << " % utilization for cores in active state";
               std::cout << "\n" << " Instructions per nominal CPU cycle: " << getTotalExecUsage(sstate1, sstate2) << " => corresponds to " << 100. * (getTotalExecUsage(sstate1, sstate2) / double(m->getMaxIPC())) << " % core utilization over time interval" << "\n";
           }
       }

       if (show_socket_output)
       {
           if (m->getNumSockets() > 1) // QPI info only for multi socket systems
           {
               std::cout << "\n" << "Intel(r) QPI data traffic estimation in bytes (data traffic coming to CPU/socket through QPI links):" << "\n" << "\n";


               const uint32 qpiLinks = (uint32)m->getQPILinksPerSocket();

               std::cout << "              ";
               for (uint32 i = 0; i < qpiLinks; ++i)
                   std::cout << " QPI" << i << "    ";

               if (m->qpiUtilizationMetricsAvailable())
               {
                   std::cout << "| ";
                   for (uint32 i = 0; i < qpiLinks; ++i)
                       std::cout << " QPI" << i << "  ";
               }

               std::cout << "\n" << "----------------------------------------------------------------------------------------------" << "\n";


               for (uint32 i = 0; i < m->getNumSockets(); ++i)
               {
                   std::cout << " SKT   " << std::setw(2) << i << "     ";
                   for (uint32 l = 0; l < qpiLinks; ++l)
                       std::cout << unit_format(getIncomingQPILinkBytes(i, l, sstate1, sstate2)) << "   ";

                   if (m->qpiUtilizationMetricsAvailable())
                   {
                       std::cout << "|  ";
                       for (uint32 l = 0; l < qpiLinks; ++l)
                           std::cout << std::setw(3) << std::dec << int(100. * getIncomingQPILinkUtilization(i, l, sstate1, sstate2)) << "%   ";
                   }

                   std::cout << "\n";
               }
           }
       }

       if (show_system_output)
       {
           std::cout << "----------------------------------------------------------------------------------------------" << "\n";

           if (m->getNumSockets() > 1) // QPI info only for multi socket systems
               std::cout << "Total QPI incoming data traffic: " << unit_format(getAllIncomingQPILinkBytes(sstate1, sstate2)) << "     QPI data traffic/Memory controller traffic: " << getQPItoMCTrafficRatio(sstate1, sstate2) << "\n";
       }

       if (show_socket_output)
       {
           if (m->getNumSockets() > 1 && (m->outgoingQPITrafficMetricsAvailable())) // QPI info only for multi socket systems
           {
               std::cout << "\n" << "Intel(r) QPI traffic estimation in bytes (data and non-data traffic outgoing from CPU/socket through QPI links):" << "\n" << "\n";


               const uint32 qpiLinks = (uint32)m->getQPILinksPerSocket();

               std::cout << "              ";
               for (uint32 i = 0; i < qpiLinks; ++i)
                   std::cout << " QPI" << i << "    ";


               std::cout << "| ";
               for (uint32 i = 0; i < qpiLinks; ++i)
                   std::cout << " QPI" << i << "  ";


               std::cout << "\n" << "----------------------------------------------------------------------------------------------" << "\n";


               for (uint32 i = 0; i < m->getNumSockets(); ++i)
               {
                   std::cout << " SKT   " << std::setw(2) << i << "     ";
                   for (uint32 l = 0; l < qpiLinks; ++l)
                       std::cout << unit_format(getOutgoingQPILinkBytes(i, l, sstate1, sstate2)) << "   ";

                   std::cout << "|  ";
                   for (uint32 l = 0; l < qpiLinks; ++l)
                       std::cout << std::setw(3) << std::dec << int(100. * getOutgoingQPILinkUtilization(i, l, sstate1, sstate2)) << "%   ";

                   std::cout << "\n";
               }

               std::cout << "----------------------------------------------------------------------------------------------" << "\n";
               std::cout << "Total QPI outgoing data and non-data traffic: " << unit_format(getAllOutgoingQPILinkBytes(sstate1, sstate2)) << "\n";
           }
       }
       if (show_socket_output)
       {
           if (m->packageEnergyMetricsAvailable())
           {
               std::cout << "\n";
               std::cout << "----------------------------------------------------------------------------------------------" << "\n";
               for (uint32 i = 0; i < m->getNumSockets(); ++i)
               {
                   std::cout << " SKT   " << std::setw(2) << i << " package consumed " << getConsumedJoules(sktstate1[i], sktstate2[i]) << " Joules\n";
               }
               std::cout << "----------------------------------------------------------------------------------------------" << "\n";
               std::cout << " TOTAL:                    " << getConsumedJoules(sstate1, sstate2) << " Joules\n";
           }
           if (m->dramEnergyMetricsAvailable())
           {
               std::cout << "\n";
               std::cout << "----------------------------------------------------------------------------------------------" << "\n";
               for (uint32 i = 0; i < m->getNumSockets(); ++i)
               {
                   std::cout << " SKT   " << std::setw(2) << i << " DIMMs consumed " << getDRAMConsumedJoules(sktstate1[i], sktstate2[i]) << " Joules\n";
               }
               std::cout << "----------------------------------------------------------------------------------------------" << "\n";
               std::cout << " TOTAL:                  " << getDRAMConsumedJoules(sstate1, sstate2) << " Joules\n";
           }
       }

   }

void display_bandwidth(float *iMC_Rd_socket_chan, float *iMC_Wr_socket_chan, float *iMC_Rd_socket, float *iMC_Wr_socket, uint32 numSockets, uint32 num_imc_channels, uint64 *partial_write)
{
    float sysRead = 0.0, sysWrite = 0.0;
    uint32 skt = 0;
    std::cout.setf(std::ios::fixed);
    std::cout.precision(2);

    while(skt < numSockets)
    {
        if(!(skt % 2) && ((skt+1) < numSockets)) //This is even socket, and it has at least one more socket which can be displayed together
        {
            std::cout << "\
                \r---------------------------------------||---------------------------------------\n\
                \r--             Socket "<<skt<<"              --||--             Socket "<<skt+1<<"              --\n\
                \r---------------------------------------||---------------------------------------\n\
                \r---------------------------------------||---------------------------------------\n\
                \r---------------------------------------||---------------------------------------\n\
                \r--   Memory Performance Monitoring   --||--   Memory Performance Monitoring   --\n\
                \r---------------------------------------||---------------------------------------\n\
                \r"; 
            for(uint64 channel = 0; channel < num_imc_channels; ++channel)
            {
                if(iMC_Rd_socket_chan[skt*num_imc_channels+channel] < 0.0 && iMC_Wr_socket_chan[skt*num_imc_channels+channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
                    continue;
                std::cout << "\r--  Mem Ch "
                    <<channel
                    <<": Reads (MB/s):"
                    <<std::setw(8)
                    <<iMC_Rd_socket_chan[skt*num_imc_channels+channel];
                std::cout <<"  --||--  Mem Ch "
                    <<channel
                    <<": Reads (MB/s):"
                    <<std::setw(8)
                    <<iMC_Rd_socket_chan[(skt+1)*num_imc_channels+channel]
                    <<"  --"
                    <<std::endl;
                std::cout << "\r--            Writes(MB/s):"
                    <<std::setw(8)
                    <<iMC_Wr_socket_chan[skt*num_imc_channels+channel];
                std::cout <<"  --||--            Writes(MB/s):"
                    <<std::setw(8)
                    <<iMC_Wr_socket_chan[(skt+1)*num_imc_channels+channel]
                    <<"  --"
                    <<std::endl;
            }
            std::cout << "\
                \r-- NODE"<<skt<<" Mem Read (MB/s):  "<<std::setw(8)<<iMC_Rd_socket[skt]<<"  --||-- NODE"<<skt+1<<" Mem Read (MB/s):  "<<std::setw(8)<<iMC_Rd_socket[skt+1]<<"  --\n\
                \r-- NODE"<<skt<<" Mem Write (MB/s): "<<std::setw(8)<<iMC_Wr_socket[skt]<<"  --||-- NODE"<<skt+1<<" Mem Write (MB/s): "<<std::setw(8)<<iMC_Wr_socket[skt+1]<<"  --\n\
                \r-- NODE"<<skt<<" P. Write (T/s) :"<<std::dec<<std::setw(10)<<partial_write[skt]<<"  --||-- NODE"<<skt+1<<" P. Write (T/s): "<<std::dec<<std::setw(10)<<partial_write[skt+1]<<"  --\n\
                \r-- NODE"<<skt<<" Memory (MB/s): "<<std::setw(11)<<std::right<<iMC_Rd_socket[skt]+iMC_Wr_socket[skt]<<"  --||-- NODE"<<skt+1<<" Memory (MB/s): "<<std::setw(11)<<iMC_Rd_socket[skt+1]+iMC_Wr_socket[skt+1]<<"  --\n\
                \r";
           sysRead += iMC_Rd_socket[skt];
           sysRead += iMC_Rd_socket[skt+1];
           sysWrite += iMC_Wr_socket[skt];
           sysWrite += iMC_Wr_socket[skt+1];
           skt += 2;
        }
        else //Display one socket in this row
        {
            std::cout << "\
                \r---------------------------------------|\n\
                \r--             Socket "<<skt<<"              --|\n\
                \r---------------------------------------|\n\
                \r---------------------------------------|\n\
                \r---------------------------------------|\n\
                \r--   Memory Performance Monitoring   --|\n\
                \r---------------------------------------|\n\
                \r"; 
            for(uint64 channel = 0; channel < num_imc_channels; ++channel)
            {
                if(iMC_Rd_socket_chan[skt*num_imc_channels+channel] < 0.0 && iMC_Wr_socket_chan[skt*num_imc_channels+channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
                    continue;
                std::cout << "--  Mem Ch "
                    <<channel
                    <<": Reads (MB/s):"
                    <<std::setw(8)
                    <<iMC_Rd_socket_chan[skt*num_imc_channels+channel]
                    <<"  --|\n--            Writes(MB/s):"
                    <<std::setw(8)
                    <<iMC_Wr_socket_chan[skt*num_imc_channels+channel]
                    <<"  --|\n";

            }
            std::cout << "\
                \r-- NODE"<<skt<<" Mem Read (MB/s):  "<<std::setw(8)<<iMC_Rd_socket[skt]<<"  --|\n\
                \r-- NODE"<<skt<<" Mem Write (MB/s) :"<<std::setw(8)<<iMC_Wr_socket[skt]<<"  --|\n\
                \r-- NODE"<<skt<<" P. Write (T/s) :"<<std::setw(10)<<std::dec<<partial_write[skt]<<"  --|\n\
                \r-- NODE"<<skt<<" Memory (MB/s): "<<std::setw(8)<<iMC_Rd_socket[skt]+iMC_Wr_socket[skt]<<"     --|\n\
                \r";

            sysRead += iMC_Rd_socket[skt];
            sysWrite += iMC_Wr_socket[skt];
            skt += 1;
        }
    }
    std::cout << "\
        \r---------------------------------------||---------------------------------------\n\
        \r--                   System Read Throughput(MB/s):"<<std::setw(10)<<sysRead<<"                  --\n\
        \r--                  System Write Throughput(MB/s):"<<std::setw(10)<<sysWrite<<"                  --\n\
        \r--                 System Memory Throughput(MB/s):"<<std::setw(10)<<sysRead+sysWrite<<"                  --\n\
        \r---------------------------------------||---------------------------------------" << std::endl;
}

void display_bandwidth_csv_header(float *iMC_Rd_socket_chan, float *iMC_Wr_socket_chan, float *iMC_Rd_socket, float *iMC_Wr_socket, uint32 numSockets, uint32 num_imc_channels, uint64 *partial_write)
{
   (void) iMC_Rd_socket; (void) iMC_Wr_socket; (void) partial_write; 
  std::cout << ";;" ; // Time

    for (uint32 skt=0; skt < numSockets; ++skt)
    {
      for(uint64 channel = 0; channel < num_imc_channels; ++channel)
   {
     if(iMC_Rd_socket_chan[skt*num_imc_channels+channel] < 0.0 && iMC_Wr_socket_chan[skt*num_imc_channels+channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
       continue;
     std::cout << "SKT" << skt << ";SKT" << skt << ';';
   }
      std::cout << "SKT"<<skt<<";"
      << "SKT"<<skt<<";"
      << "SKT"<<skt<<";"
      << "SKT"<<skt<<";";
      
    }
    std::cout << "System;System;System\n";
      

  std::cout << "Date;Time;" ;
    for (uint32 skt=0; skt < numSockets; ++skt)
    {
      for(uint64 channel = 0; channel < num_imc_channels; ++channel)
   {
     if(iMC_Rd_socket_chan[skt*num_imc_channels+channel] < 0.0 && iMC_Wr_socket_chan[skt*num_imc_channels+channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
       continue;
     std::cout << "Ch" <<channel <<"Read;"
          << "Ch" <<channel <<"Write;";
   }
      std::cout << "Mem Read (MB/s);Mem Write (MB/s); P. Write (T/s); Memory (MB/s);";
    }

    std::cout << "Read;Write;Memory" << std::endl;
}

void display_bandwidth_csv(float *iMC_Rd_socket_chan, float *iMC_Wr_socket_chan, float *iMC_Rd_socket, float *iMC_Wr_socket, uint32 numSockets, uint32 num_imc_channels, uint64 *partial_write, uint64 elapsedTime)
{
   (void) elapsedTime;
    time_t t = time(NULL);
    tm *tt = localtime(&t);
    std::cout.precision(3);
    std::cout << 1900+tt->tm_year << '-' << 1+tt->tm_mon << '-' << tt->tm_mday << ';'
         << tt->tm_hour << ':' << tt->tm_min << ':' << tt->tm_sec << ';';


    float sysRead = 0.0, sysWrite = 0.0;

    std::cout.setf(std::ios::fixed);
    std::cout.precision(2);

    for (uint32 skt=0; skt < numSockets; ++skt)
     {
       for(uint64 channel = 0; channel < num_imc_channels; ++channel)
    {
      if(iMC_Rd_socket_chan[skt*num_imc_channels+channel] < 0.0 && iMC_Wr_socket_chan[skt*num_imc_channels+channel] < 0.0) //If the channel read neg. value, the channel is not working; skip it.
        continue;
      std::cout <<std::setw(8) <<iMC_Rd_socket_chan[skt*num_imc_channels+channel] << ';'
      <<std::setw(8) <<iMC_Wr_socket_chan[skt*num_imc_channels+channel] << ';';
      
    }
       std::cout <<std::setw(8) <<iMC_Rd_socket[skt] <<';'
            <<std::setw(8) <<iMC_Wr_socket[skt] <<';'
            <<std::setw(10) <<std::dec<<partial_write[skt] <<';'
            <<std::setw(8) <<iMC_Rd_socket[skt]+iMC_Wr_socket[skt] <<';';

            sysRead += iMC_Rd_socket[skt];
            sysWrite += iMC_Wr_socket[skt];
    }

    std::cout <<std::setw(10) <<sysRead <<';'
    <<std::setw(10) <<sysWrite <<';'
    <<std::setw(10) <<sysRead+sysWrite << std::endl;
}

void calculate_bandwidth(PCM *m, const ServerUncorePowerState uncState1[], const ServerUncorePowerState uncState2[], uint64 elapsedTime, bool csv, bool & csvheader)
{
    //const uint32 num_imc_channels = m->getMCChannelsPerSocket();
    float iMC_Rd_socket_chan[MAX_SOCKETS][MAX_IMC_CHANNELS];
    float iMC_Wr_socket_chan[MAX_SOCKETS][MAX_IMC_CHANNELS];
    float iMC_Rd_socket[MAX_SOCKETS];
    float iMC_Wr_socket[MAX_SOCKETS];
    uint64 partial_write[MAX_SOCKETS];

    for(uint32 skt = 0; skt < m->getNumSockets(); ++skt)
    {
        iMC_Rd_socket[skt] = 0.0;
        iMC_Wr_socket[skt] = 0.0;
        partial_write[skt] = 0;

        for(uint32 channel = 0; channel < MAX_IMC_CHANNELS; ++channel)
        {
            if(getMCCounter(channel,READ,uncState1[skt],uncState2[skt]) == 0.0 && getMCCounter(channel,WRITE,uncState1[skt],uncState2[skt]) == 0.0) //In case of JKT-EN, there are only three channels. Skip one and continue.
            {
                iMC_Rd_socket_chan[skt][channel] = -1.0;
                iMC_Wr_socket_chan[skt][channel] = -1.0;
                continue;
            }

            iMC_Rd_socket_chan[skt][channel] = (float) (getMCCounter(channel,READ,uncState1[skt],uncState2[skt]) * 64 / 1000000.0 / (elapsedTime/1000.0));
            iMC_Wr_socket_chan[skt][channel] = (float) (getMCCounter(channel,WRITE,uncState1[skt],uncState2[skt]) * 64 / 1000000.0 / (elapsedTime/1000.0));

            iMC_Rd_socket[skt] += iMC_Rd_socket_chan[skt][channel];
            iMC_Wr_socket[skt] += iMC_Wr_socket_chan[skt][channel];

            partial_write[skt] += (uint64) (getMCCounter(channel,PARTIAL,uncState1[skt],uncState2[skt]) / (elapsedTime/1000.0));
        }
    }

    if (csv) {
      if (csvheader) {
   display_bandwidth_csv_header(iMC_Rd_socket_chan[0], iMC_Wr_socket_chan[0], iMC_Rd_socket, iMC_Wr_socket, m->getNumSockets(), MAX_IMC_CHANNELS, partial_write);
   csvheader = false;
      }
      display_bandwidth_csv(iMC_Rd_socket_chan[0], iMC_Wr_socket_chan[0], iMC_Rd_socket, iMC_Wr_socket, m->getNumSockets(), MAX_IMC_CHANNELS, partial_write, elapsedTime);
    } else {
      display_bandwidth(iMC_Rd_socket_chan[0], iMC_Wr_socket_chan[0], iMC_Rd_socket, iMC_Wr_socket, m->getNumSockets(), MAX_IMC_CHANNELS, partial_write);
    }
}



/////////////////////////////////////////////////////
   void print_state() {
      const bool show_core_output = true;
      const bool show_socket_output = true;
      const bool show_system_output = true;
      
      print_output(m, cstates1, cstates2, sktstate1, sktstate2, sstate1, sstate2,
      cpu_model, show_core_output, show_socket_output, show_system_output);

      bool csv = false;
      bool csvheader = false;

      calculate_bandwidth(m,BeforeState,AfterState,AfterTime-BeforeTime,csv,csvheader);

      std::cout << "BRANCH INTRUCTIONS RETIRED: " << BRANCH_INSTR_RETIRED_events << std::endl;
      std::cout << "BRANCH MISSES RETIRED: " << BRANCH_MISSES_RETIRED_events << std::endl;

   }
}; //static rpcm;

#endif
