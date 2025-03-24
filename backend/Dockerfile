# https://hub.docker.com/layers/library/python/3.13.1-bookworm/images/sha256-8eed0b2dee7938ad0e39fbdd1aa8bc026501e4c3f829a8e4512db690a68a90f9?context=explore
FROM python:3.13.1-bookworm@sha256:3b1b63f17c5197411ee572be110333dae4b9d6f2fbc4f84c790f644e791d356b

SHELL ["/bin/bash", "-c"]

RUN apt update && apt upgrade -y

COPY . /app

WORKDIR /app

RUN make install

EXPOSE 8000

CMD ["make", "configure-and-run"]
