FROM alpine:3.14
# dependencies needed for compiling c extensions
# also busybox-extras for telnet for easier use of backdoor
RUN apk --update add py3-pip g++ python3-dev libffi-dev musl-dev postgresql-dev file make busybox-extras

# Try to get wheels working
RUN pip install --upgrade pip wheel

# Install gevent and bokeh so that we don't need to re-install them when common changes
RUN pip install gevent==22.10.2
RUN pip install bokeh

RUN apk add postgresql-dev postgresql-libs

# Install common lib first as it changes less
COPY common /tmp/common
RUN pip install /tmp/common && rm -r /tmp/common

COPY graphs /tmp/graphs
RUN pip install /tmp/graphs && rm -r /tmp/graphs

LABEL org.opencontainers.image.source https://github.com/dbvideostriketeam/wubloader
ENTRYPOINT ["python3", "-m", "graphs"]
