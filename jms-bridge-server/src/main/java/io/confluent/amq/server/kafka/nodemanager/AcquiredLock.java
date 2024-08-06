package io.confluent.amq.server.kafka.nodemanager;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.jackson.Jacksonized;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Jacksonized
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class AcquiredLock {
    String lockHolderId;
    int acquisitionEpoch;
}
