ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG

FROM ${DOCKER_UPSTREAM_REGISTRY}confluentinc/cp-base-new:${DOCKER_UPSTREAM_TAG}

ARG PROJECT_VERSION
ARG ARTIFACT_ID

USER root

ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/java/${ARTIFACT_ID}/* /usr/share/java/jms-bridge/
ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/doc/${ARTIFACT_ID}/* /usr/share/doc/jms-bridge/
ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/share/java/jmx_prometheus_agent/* /usr/share/java/jmx_prometheus_agent/
ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/bin/* /usr/bin/
ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/etc/* /etc/jms-bridge/
ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/docker/etc/* /etc/jms-bridge/
ADD --chown=appuser:appuser target/${ARTIFACT_ID}-${PROJECT_VERSION}-package/docker/bin/* /usr/bin/docker/

RUN chmod +x /usr/bin/docker/run /usr/bin/jms-bridge*

USER appuser

CMD ["/usr/bin/docker/run"]