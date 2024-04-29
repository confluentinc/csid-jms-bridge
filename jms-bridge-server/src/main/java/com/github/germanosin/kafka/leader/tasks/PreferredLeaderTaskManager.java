package com.github.germanosin.kafka.leader.tasks;

import com.github.germanosin.kafka.leader.HostMemberIdentity;
import com.github.germanosin.kafka.leader.MemberIdentity;
import org.jgroups.util.TimeScheduler;

import java.util.*;
import java.util.function.Consumer;

import static com.github.germanosin.kafka.leader.tasks.TaskAssignment.CURRENT_VERSION;
import static com.github.germanosin.kafka.leader.tasks.TaskAssignment.NO_ERROR;

public class PreferredLeaderTaskManager  extends DefaultLeaderTasksManager<HostMemberIdentity> {

    public static final String LIVE_TASK = "live";
    public static final String BACKUP_TASK = "backup";

    public PreferredLeaderTaskManager(Map<String, ? extends Task> tasks) {
        super(tasks);
    }

    public PreferredLeaderTaskManager(Map<String, ? extends Task> tasks, Consumer<DefaultLeaderTasksManager<?>> terminationFunction) {
        super(tasks, terminationFunction);
    }

    @Override
    public Map<HostMemberIdentity, TaskAssignment> assign(List<HostMemberIdentity> identities) {
        Map<HostMemberIdentity, TaskAssignment> results = new HashMap<>();
        //two tasks, backup and live
        //if preferred live is available then it is live
        // if it isn't then backup is live and there is no backup
        Set<HostMemberIdentity> remainingHosts = new HashSet<>(identities);

        //find preferred live
        HostMemberIdentity live =
                identities.stream().filter(i -> i.getEligibleTasks().contains(LIVE_TASK))
                        .findFirst()
                        .orElseGet(() -> identities.get(0));

        TaskAssignment template = TaskAssignment.builder()
                .version(CURRENT_VERSION)
                .error(NO_ERROR)
                .build();

        results.put(
                live,
                template.toBuilder().tasks(Collections.singletonList(LIVE_TASK)).build());
        remainingHosts.remove(live);
        remainingHosts.forEach(host ->
                results.put(host,
                        template.toBuilder().tasks(Collections.singletonList(BACKUP_TASK)).build()));
        return results;
    }

    @Override
    public boolean isEligible(List<HostMemberIdentity> identities, HostMemberIdentity candidate, String taskKey) {

        if (candidate.getEligibleTasks().contains(taskKey)) {
            return true;
        }

        if (identities.stream().noneMatch(c -> c.getEligibleTasks().contains(taskKey))) {
            return true;
        }

        return false;
    }
}
