FROM golemfactory/base:1.5 as base

FROM base as blender

ENV BLENDER_VERSION 2.82
ENV BLENDER_OS linux64
ENV BLENDER_BASENAME blender-$BLENDER_VERSION-$BLENDER_OS
# The mirror is recently faster than the official site.
# ENV BLENDER_BZ2_URL http://download.blender.org/release/Blender$BLENDER_MAJOR/blender-$BLENDER_VERSION-linux-glibc$GLIBC_VERSION-x86_64.tar.bz2
ENV BLENDER_ARCHIVE_URL http://mirror.cs.umn.edu/blender.org/release/Blender$BLENDER_VERSION/$BLENDER_BASENAME.tar.xz

RUN curl -Ls ${BLENDER_ARCHIVE_URL} | tar -xJv -C / \
  && mv /$BLENDER_BASENAME /blender \
  && rm -rf /blender/$BLENDER_VERSION/datafiles/locale


FROM blender as builder

RUN apt-get update \
  && apt-get install -y \
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

# Check to see that all libraries are available
RUN ! ldd /blender/blender | grep "=> not found"

RUN apt-get install -y python3-pip python3-venv
RUN python3 -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH=/opt/venv/bin:$PATH
RUN pip install --upgrade pip
COPY golem_blender_app/requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

# Use following lines to work with the unreleased task-api module
# RUN apt-get install -y git
# RUN pip uninstall -y golem-task-api
# ENV GITCOMMIT=bfc2872c44d89132e43667d826b53e666c6453dc
# RUN pip install git+https://github.com/golemfactory/task-api@$GITCOMMIT#subdirectory=python

# Check to see that all libraries are available
# We don't need libgfortran
RUN ! find /opt/venv/lib64/python3.6/site-packages -name "*.so" | xargs ldd | grep -v "libgfortran-ed201abd.so.3.0.0" | grep "=> not found"
RUN mkdir /golem-libs
# Find which libraries are used by Blender and python modules and copy them to /golem-libs
RUN { find /opt/venv/lib64/python3.6/site-packages -name "*.so"; echo "/blender/blender"; } | xargs ldd | grep "=> /usr/lib/x86_64-linux-gnu" | awk '{$1=$1};1' | sort | rev | uniq -f 2 | rev | cut -f 3 -d" " | xargs cp -t /golem-libs/

COPY golem_blender_app /golem/golem_blender_app
RUN pip install /golem/golem_blender_app

RUN find /opt/venv -regex '^.*\(__pycache__\|\.py[co]\)$' -delete


FROM base as missing-libs

RUN mkdir /golem-libs
COPY --from=builder /golem-libs /golem-libs
WORKDIR /golem-libs
RUN for lib in `ls`; do ldconfig -p | grep -q $lib && rm $lib; done || true


FROM base

MAINTAINER Golem Tech <tech@golem.network>

COPY --from=blender /blender /blender
ENV PATH=/blender:$PATH

ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

COPY benchmark /golem/benchmark

ENTRYPOINT ["python", "-m", "golem_blender_app.entrypoint"]

WORKDIR /golem/work

COPY --from=missing-libs /golem-libs/ /usr/local/lib/
COPY --from=builder /lib/x86_64-linux-gnu/libbsd.so.0 /usr/local/lib/

COPY --from=builder /opt/venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH=/opt/venv/bin:$PATH
