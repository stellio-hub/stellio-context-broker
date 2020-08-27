ARG GITHUB_ACCOUNT=stellio-hub
ARG GITHUB_REPOSITORY=stellio-context-broker
ARG DOWNLOAD=latest
ARG SOURCE_BRANCH=develop
ARG JAVA_VERSION=11

########################################################################################
#
# This build stage retrieves the source code from GitHub. The default download is the 
# latest tip of the master of the named repository on GitHub.
#
######################################################################################## 

FROM openjdk:${JAVA_VERSION}-jdk-slim AS builder
ARG GITHUB_ACCOUNT
ARG GITHUB_REPOSITORY
ARG DOWNLOAD
ARG SOURCE_BRANCH

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# hadolint ignore=DL3008
RUN \
	# Ensure that unzip is installed prior to downloading
	apt-get update && \
	apt-get install -y --no-install-recommends unzip curl && \
	if [ "${DOWNLOAD}" = "latest" ] ; \
	then \
		RELEASE="${SOURCE_BRANCH}"; \
		echo "INFO: Building Latest Development from ${SOURCE_BRANCH} branch."; \
	elif [ "${DOWNLOAD}" = "stable" ]; \
	then \
		RELEASE=$(curl -s https://api.github.com/repos/"${GITHUB_ACCOUNT}"/"${GITHUB_REPOSITORY}"/releases/latest | grep 'tag_name' | cut -d\" -f4); \
		echo "INFO: Building Latest Stable Release: ${RELEASE}"; \
	else \
	 	RELEASE="${DOWNLOAD}"; \
	 	echo "INFO: Building Release: ${RELEASE}"; \
	fi && \
	RELEASE_CONCAT=$(echo "${RELEASE}" | tr / -); \
	curl -s -L https://github.com/"${GITHUB_ACCOUNT}"/"${GITHUB_REPOSITORY}"/archive/"${RELEASE}".zip > source.zip && \
	unzip source.zip && \
	rm source.zip && \
	if ! case $DOWNLOAD in v*) false;; esac; \
	then \
    	mv "${GITHUB_REPOSITORY}-${RELEASE_CONCAT#?}" /source; \
    else \
    	mv "${GITHUB_REPOSITORY}-${RELEASE_CONCAT}" /source; \
	fi

WORKDIR /source
RUN ./gradlew build -p api-gateway
COPY build/libs/*.jar /source/app.jar

########################################################################################
#
# This build stage creates an anonymous user to be used with the distroless build
# as defined below.
#
########################################################################################
FROM openjdk:${JAVA_VERSION}-jdk-slim AS anon-user
RUN sed -i -r "/^(root|nobody)/!d" /etc/passwd /etc/shadow /etc/group \
    && sed -i -r 's#^(.*):[^:]*$#\1:/sbin/nologin#' /etc/passwd

########################################################################################
#
# This build stage creates a distroless image for production.
#
########################################################################################

FROM gcr.io/distroless/java:${JAVA_VERSION}
ARG GITHUB_ACCOUNT
ARG GITHUB_REPOSITORY
ARG JAVA_VERSION

LABEL "maintainer"="EGM"
LABEL "org.opencontainers.image.authors"="EGM"
LABEL "org.opencontainers.image.documentation"="https://stellio.readthedocs.io/"
LABEL "org.opencontainers.image.vendor"="EGM"
LABEL "org.opencontainers.image.licenses"="Apache-2.0"
LABEL "org.opencontainers.image.title"="Stellio context broker"
LABEL "org.opencontainers.image.description"="Stellio is an NGSI-LD compliant context broker developed by EGM. NGSI-LD is an Open API and Datamodel specification for context management published by ETSI."
LABEL "org.opencontainers.image.source"="https://github.com/${GITHUB_ACCOUNT}/${GITHUB_REPOSITORY}"
LABEL "com.java.version"="${JAVA_VERSION}"

COPY --from=anon-user /etc/passwd /etc/shadow /etc/group /etc/
COPY --from=builder /source/app.jar /app/app.jar
WORKDIR /app

USER nobody
CMD ["app.jar"]