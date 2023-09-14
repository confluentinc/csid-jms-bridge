package io.confluent.amq.config;

import io.confluent.csid.common.utils.accelerator.Accelerator;
import io.confluent.csid.common.utils.accelerator.ClientId;
import io.confluent.csid.common.utils.accelerator.Owner;
import org.apache.commons.lang3.StringUtils;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

import static io.confluent.amq.config.BridgeConfigFactory.getBridgeVersion;

@FreeBuilder
public interface BridgeClientId {
    String bridgeId();
    String acceleratorOwner();
    Optional<String> partnerSFDCId();
    String acceleratorId();
    String acceleratorVersion();

    Optional<String> moreClientId();

    default String clientId(String evenMoreClientId) {
        return ClientId.genClientId(
                acceleratorOwner(),
                partnerSFDCId().orElse(""),
                acceleratorId(),
                acceleratorVersion(),
                String.format("%s%s%s",
                        bridgeId(),
                        moreClientId().map(s -> "_" + s).orElse(""),
                        StringUtils.isNotBlank(evenMoreClientId) ? ("_" + evenMoreClientId) : ""));

    }

    default String clientId() {
        return clientId("");
    }

    default BridgeClientId withEvenMoreClientId(String evenMoreClientId) {
        return new Builder().mergeFrom(this)
                .moreClientId(moreClientId().map(s -> s + "_" + evenMoreClientId).orElse(evenMoreClientId))
                .build();
    }

    class Builder extends BridgeClientId_Builder {

        public Builder() {
            acceleratorId(Accelerator.JMS_BRIDGE.getAcceleratorId());
            acceleratorOwner(Owner.PIE_LABS.name());
            acceleratorVersion(BridgeConfigFactory.getBridgeVersion());
        }
    }
}
