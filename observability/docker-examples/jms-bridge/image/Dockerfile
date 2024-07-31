FROM confluentinc/cp-base-new:latest

ARG PROJECT_VERSION=3.3.1
ARG ARTIFACT_ID=jms-bridge
ARG UNZIPPED_FOLDER_331=jms-bridge-server-3.3.1
ARG UNZIPPED_FOLDER=jms-bridge-server-3.3.4-SNAPSHOT-development

USER root

ADD --chown=appuser:appuser ${UNZIPPED_FOLDER}/share/java/${ARTIFACT_ID}/* /usr/share/java/jms-bridge/
ADD --chown=appuser:appuser ${UNZIPPED_FOLDER_331}/lib/${ARTIFACT_ID}/jmx-prometheus-javaagent/* /usr/share/java/jmx_prometheus_agent/
ADD --chown=appuser:appuser ${UNZIPPED_FOLDER}/bin/* /usr/bin/
ADD --chown=appuser:appuser ${UNZIPPED_FOLDER}/etc/* /etc/jms-bridge/
ADD --chown=appuser:appuser ./docker/etc/* /etc/jms-bridge/
ADD --chown=appuser:appuser ./docker/bin/* /usr/bin/docker/

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
ENV PROMETHEUS_ENABLED=Y
ENV PROMETHEUS_PORT=8888

CMD ["/usr/bin/docker/run"]