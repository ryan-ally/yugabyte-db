package com.yugabyte.yw.commissioner.tasks;

import static com.yugabyte.yw.forms.UniverseTaskParams.isFirstTryForTask;

import com.yugabyte.yw.commissioner.BaseTaskDependencies;
import com.yugabyte.yw.commissioner.ITask.Abortable;
import com.yugabyte.yw.commissioner.ITask.Retryable;
import com.yugabyte.yw.commissioner.tasks.subtasks.KubernetesCommandExecutor;
import com.yugabyte.yw.commissioner.UserTaskDetails.SubTaskGroupType;
import com.yugabyte.yw.common.PlacementInfoUtil;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.Cluster;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.PlacementInfo;
import java.util.Collections;
import java.util.List;
import com.yugabyte.yw.models.Provider;
import java.util.Set;
import java.util.UUID;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

// Tracks the read only kubernetes cluster create intent within an existing universe.
@Slf4j
@Abortable
@Retryable
public class ReadOnlyKubernetesCluterCreate extends KubernetesTaskBase {
  @Inject
  protected ReadOnlyKubernetesCluterCreate(BaseTaskDependencies baseTaskDependencies) {
    super(baseTaskDependencies);
  }

  @Override
  public void run() {
    log.info("Started {} task for uuid={}", getName(), taskParams().universeUUID);
    try {
        Universe universe = lockUniverseForUpdate(taskParams().expectedUniverseVersion);

        // Set all the in-memory node names first.
        setNodeNames(universe);

        // TODO remove
        List<Cluster> list = taskParams().getReadOnlyClusters();
        if(list.size()!=1) {
            log.info("Expected 1 read only cluster but found {}", list.size());
            return;
        }
        //

        Cluster readOnlyCluster = taskParams().getReadOnlyClusters().get(0);
        PlacementInfo pi = readOnlyCluster.placementInfo;
        writeUserIntentToUniverse(true);

        Provider provider =
            Provider.get(UUID.fromString(readOnlyCluster.userIntent.provider));

        KubernetesPlacement placement = new KubernetesPlacement(pi);
        if(!placement.masters.isEmpty()) {
            // TODO throw exception
            log.info("Expected 0 masters in read only cluster but found {}", placement.masters.size());
            return;
        }
        boolean isMultiAz = PlacementInfoUtil.isMultiAZ(provider);
        createPodsTask(placement, null, true);
        
        // Following method assumes primary cluster. TODO figure out what it does!
        //createSingleKubernetesExecutorTask(KubernetesCommandExecutor.CommandType.POD_INFO, pi);
 
        Set<NodeDetails> tserversAdded =
            getPodsToAdd(placement.tservers, null, ServerType.TSERVER, isMultiAz);
        
        // Wait for new tablet servers to be responsive.
        createWaitForServersTasks(tserversAdded, ServerType.TSERVER)
            .setSubTaskGroupType(SubTaskGroupType.ConfigureUniverse);

        // Persist the placement info into the YB master leader.
        createPlacementInfoTask(null /* blacklistNodes */)
            .setSubTaskGroupType(SubTaskGroupType.ConfigureUniverse);
        
        // Wait for a master leader to hear from all the tservers.
        createWaitForTServerHeartBeatsTask().setSubTaskGroupType(SubTaskGroupType.ConfigureUniverse);

        createSwamperTargetUpdateTask(false);
        // Marks the update of this universe as a success only if all the tasks before it succeeded.
        createMarkUniverseUpdateSuccessTasks()
            .setSubTaskGroupType(SubTaskGroupType.ConfigureUniverse);

        // Run all the tasks.
        getRunnableTask().runSubTasks();
    } catch (Throwable t) {
        log.error("Error executing task {}, error='{}'", getName(), t.getMessage(), t);
        throw t;
    } finally {
        unlockUniverseForUpdate();
    }
    log.info("Finished {} task.", getName());
  }
}
