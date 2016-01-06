# EmptyHeaded Docker for Ubuntu 12.04 LTS
FROM ubuntu:12.04
MAINTAINER Christopher Aberger <craberger@gmail.com>

# update apt repositories
RUN apt-get update

# install add-apt-repository tool
RUN apt-get -y install software-properties-common
RUN apt-get install python-software-properties -y

# install wget for downloading files
RUN apt-get install -y wget

# Install Java.
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN add-apt-repository ppa:ubuntu-toolchain-r/test && apt-get update && apt-get install -y \
    build-essential \
    git \
    python \
    make \
    python-dev \
    python-pip \
    g++-4.9 \
    libtbb-dev \
    clang-format-3.4 \
    vim \
    pkg-config \
    screen \
    libjemalloc-dev 

RUN apt-get install -y firefox

#install scala
RUN wget https://dl.bintray.com/sbt/debian/sbt-0.13.7.deb
RUN dpkg -i sbt-0.13.7.deb
RUN apt-get install sbt

#install python add ons
RUN pip install pandas 
RUN pip install ipython

RUN git clone https://github.com/HazyResearch/EmptyHeaded.git
RUN cd EmptyHeaded && ./setup.sh #pulls scala
RUN mv /usr/bin/clang-format-3.4 /usr/bin/clang-format
