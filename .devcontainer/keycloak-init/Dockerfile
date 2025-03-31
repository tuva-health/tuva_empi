# https://hub.docker.com/layers/library/python/3.13.1-bookworm/images/sha256-8eed0b2dee7938ad0e39fbdd1aa8bc026501e4c3f829a8e4512db690a68a90f9?context=explore
FROM python:3.13.1-bookworm@sha256:3b1b63f17c5197411ee572be110333dae4b9d6f2fbc4f84c790f644e791d356b

# Install curl, unzip, Java, etc.
RUN apt-get update && apt-get install -y \
    curl unzip openjdk-17-jre-headless bash jq && \
    rm -rf /var/lib/apt/lists/*

# Download Keycloak CLI tools
ENV KC_VERSION=24.0.1
RUN curl -fsSL https://github.com/keycloak/keycloak/releases/download/${KC_VERSION}/keycloak-${KC_VERSION}.zip \
    -o /tmp/keycloak.zip && \
    unzip /tmp/keycloak.zip -d /opt && \
    rm /tmp/keycloak.zip && \
    mv /opt/keycloak-${KC_VERSION} /opt/keycloak

ENV PATH="/opt/keycloak/bin:$PATH"

WORKDIR /opt/keycloak

COPY configure-keycloak.sh /opt/keycloak/configure-keycloak.sh

ENTRYPOINT ["/bin/bash", "/opt/keycloak/configure-keycloak.sh"]
