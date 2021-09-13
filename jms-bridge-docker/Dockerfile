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

RUN chmod +x /usr/bin/docker/run /usr/bin/jms-bridge* \
    && mkdir -p /var/cache/jms-bridge/streams \
    && chown -R appuser:appuser /var/cache/jms-bridge \
    && chmod -R 755 /var/cache/jms-bridge \
    && mkdir -p /var/log/jms-bridge \
    && chown appuser:appuser /var/log/jms-bridge \
    && chmod -R 755 /var/log/jms-bridge


USER appuser

ENV LOG_DIR=/var/log/jms-bridge
ENV JMS_BRIDGE_LIB_DIR=/user/share/java/jms-bridge
ENV JMS_BRIDGE_CONFIG_DIR=/etc/jms-bridge
ENV JMS_BRIDGE_DATA_DIR=/var/cache/jms-bridge
ENV JMSBRIDGE_KAFKA_STATE_DIR=/var/cache/jms-bridge/streams/

CMD ["/usr/bin/docker/run"]