FROM golemfactory/base:1.4

MAINTAINER Golem Tech <tech@golem.network>

ADD entrypoints/scripts/verifier_tools/requirements.txt /golem/work/
ADD entrypoints/scripts/verifier_tools/copy.sh /golem/

RUN apt-get update && \
  apt-get install -y \
    curl \
    bzip2 \
    libfreetype6 \
    libgl1-mesa-dev \
    libglu1-mesa \
    libxi6 \
    libxrender1 \
    libglib2.0-0 \
    g++ \
    libsm6 \
    zlib1g-dev \
    libopenexr-dev && \
    /golem/install_py_libs.sh /golem/work/requirements.txt && \
    /golem/copy.sh && \
  apt-get -y autoremove && \
  rm -rf /var/lib/apt/lists/*

# Create symbolic link to python. I don't know where, something removes it.
RUN ln -s /usr/bin/python3.6 /usr/bin/python3

ENV BLENDER_MAJOR 2.79
ENV BLENDER_VERSION 2.79
ENV GLIBC_VERSION 219
ENV BLENDER_BZ2_URL http://download.blender.org/release/Blender$BLENDER_MAJOR/blender-$BLENDER_VERSION-linux-glibc$GLIBC_VERSION-x86_64.tar.bz2
# ENV BLENDER_BZ2_URL http://mirror.cs.umn.edu/blender.org/release/Blender$BLENDER_MAJOR/blender-$BLENDER_VERSION-linux-glibc211-x86_64.tar.bz2

RUN curl -Ls ${BLENDER_BZ2_URL} | tar -xjv -C / && \
    mv /blender-${BLENDER_VERSION}-linux-glibc${GLIBC_VERSION}-x86_64 /blender

ENV PATH=/blender:/usr/bin/:$PATH

ADD entrypoints/ /golem/entrypoints/

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

WORKDIR /golem/work

COPY benchmark /golem/benchmark

ENTRYPOINT ["/usr/local/bin/entrypoint.sh", "python3", "/golem/entrypoints/app_entrypoint.py"]