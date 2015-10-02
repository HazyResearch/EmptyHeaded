//
//  Hello World client in C++
//  Connects REQ socket to tcp://localhost:5555
//  Sends "Hello" to server, expects "World" back
//
#include "zmq.hpp"
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

int main ()
{
  //  Prepare our context and socket
  zmq::context_t context(1);
  zmq::socket_t socket(context, ZMQ_REQ);

  std::cout << "connecting to server…" << std::endl << std::endl;
  socket.connect("tcp://localhost:5555");

  //  Do 10 requests, waiting each time for a response
  for (int request_nbr = 0; request_nbr != 10; request_nbr++) {

    // Read the data we'll want to send.
    std::ifstream f("src/run.cpp");
    std::stringstream buffer;
    buffer << f.rdbuf();
    const std::string data = buffer.str();

    // Build the request.
    zmq::message_t request(data.length() + 1);
    memcpy((void*)request.data(), data.c_str(), data.length() + 1);

    std::cout << "sending request..." << std::endl;
    socket.send(request);

    //  Get the reply.
    zmq::message_t reply;
    socket.recv(&reply);
    std::cout << "received reply: " << (char*)reply.data() << std::endl << std::endl;
  }

  return 0;
}
