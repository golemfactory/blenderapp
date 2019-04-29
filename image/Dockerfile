FROM golemfactory/base:1.4 as blender

ENV BLENDER_MAJOR 2.79
ENV BLENDER_VERSION 2.79
ENV GLIBC_VERSION 219
ENV BLENDER_BZ2_URL http://download.blender.org/release/Blender$BLENDER_MAJOR/blender-$BLENDER_VERSION-linux-glibc$GLIBC_VERSION-x86_64.tar.bz2
# ENV BLENDER_BZ2_URL http://mirror.cs.umn.edu/blender.org/release/Blender$BLENDER_MAJOR/blender-$BLENDER_VERSION-linux-glibc211-x86_64.tar.bz2

RUN curl -Ls ${BLENDER_BZ2_URL} | tar -xjv -C / && \
    mv /blender-${BLENDER_VERSION}-linux-glibc${GLIBC_VERSION}-x86_64 /blender


FROM golemfactory/base:1.4 as all-libs

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
    libopenexr-dev

COPY --from=blender /blender /blender
# Check to see that all libraries are available
RUN ! ldd /blender/blender | grep "=> not found"

COPY entrypoints/scripts/verifier_tools/requirements.txt /tmp
RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pip
RUN pip install -r /tmp/requirements.txt
WORKDIR /usr/local/lib/python3.6/dist-packages
# Check to see that all libraries are available
# We don't need libgfortran
RUN ! find . -name "*.so" | xargs ldd | grep -v "libgfortran-ed201abd.so.3.0.0" | grep "=> not found"

RUN mkdir /golem-libs
# Find which libraries are used by Blender and python modules and copy them to /golem-libs
RUN { find . -name "*.so"; echo "/blender/blender"; } | xargs ldd | grep "=> /usr/lib/x86_64-linux-gnu" | awk '{$1=$1};1' | sort | rev | uniq -f 2 | rev | cut -f 3 -d" " | xargs cp -t /golem-libs/


FROM golemfactory/base:1.4 as missing-libs

RUN mkdir /golem-libs
COPY --from=all-libs /golem-libs /golem-libs
WORKDIR /golem-libs
RUN for lib in `ls`; do ldconfig -p | grep -q $lib && rm $lib; done || true


FROM golemfactory/base:1.4

MAINTAINER Golem Tech <tech@golem.network>

COPY --from=missing-libs /golem-libs/ /usr/local/lib/
COPY --from=all-libs /lib/x86_64-linux-gnu/libbsd.so.0 /usr/local/lib/

COPY entrypoints/scripts/verifier_tools/requirements.txt /tmp
RUN apt-get update && \
  apt-get install -y libopenexr-dev zlib1g-dev && \
  /golem/install_py_libs.sh /tmp/requirements.txt && \
  apt-get remove -y zlib1g-dev && \
  apt-get clean && \
  apt-get -y autoremove && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /root/.cache

# Create symbolic link to python. I don't know where, something removes it.
RUN ln -s /usr/bin/python3.6 /usr/bin/python3

COPY --from=blender /blender /blender

ENV PATH=/blender:/usr/bin/:$PATH

ADD entrypoints/ /golem/entrypoints/

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

WORKDIR /golem/work

COPY benchmark /golem/benchmark

ENTRYPOINT ["/usr/local/bin/entrypoint.sh", "python3", "/golem/entrypoints/app_entrypoint.py"]