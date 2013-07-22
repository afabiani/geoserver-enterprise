/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.opengis.feature.type.Name;

/**
 * Generic interface for a process status storage. Used by ClusterProcessManager to persist process status on a shared storage used by all cluster
 * instances.
 * 
 * @author "Mauro Bartolomeoli - mauro.bartolomeoli@geo-solutions.it"
 * 
 */
public interface ProcessStorage {

    /**
     * Creates / Updates a process status.
     * 
     * @param clusterId id of the cluster instance executing the process
     * @param executionId process id
     * @param status current process status
     */
    public void putStatus(String clusterId, String executionId, ExecutionStatus status);

    /**
     * Retrieves the status of a process from the storage.
     * 
     * @param executionId process id
     * @return
     */
    public ExecutionStatus getStatus(String clusterId, String executionId);

    /**
     * Retrieves the status of the process from the storage.
     * 
     * @param executionId
     */
    public List<ExecutionStatusEx> getStatus(String executionId);

    /**
     * Removes the status of a process from the storage. The last status is returned.
     * 
     * @param executionId process id
     * @return
     */
    public ExecutionStatus removeStatus(String clusterId, String executionId);

    /**
     * Gets the status of all executing processes on all the instances of the cluster.
     * 
     * @return
     */
    public Collection<ExecutionStatus> getAll();

    /**
     * Updates the phase of a process.
     * 
     * @param executionId
     * @param phase
     */
    public void updatePhase(String clusterId, String executionId, ProcessState phase);

    /**
     * Updates the progress of a process.
     * 
     * @param executionId
     * @param progress
     */
    public void updateProgress(String clusterId, String executionId, float progress);

    /**
     * Retrieves the output of a process, with the given max timeout.
     * 
     * @param executionId
     * @param timeout
     * @return
     */
    public Map<String, Object> getOutput(String clusterId, String executionId, long timeout);

    /**
     * Gets the id of the instance executing a process.
     * 
     * @param executionId
     * @return
     */
    public String getInstance(String executionId);

    /**
     * Puts the output of a process on the storage.
     * 
     * @param executionId
     * @param output
     */
    public void putOutput(String clusterId, String executionId, ExecutionStatus status);

    /**
     * Puts the output error of a process on the storage.
     * 
     * @param executionId
     * @param e
     */
    public void putOutput(String clusterId, String executionId, Exception e);

    /**
     * 
     * @param executionId
     * @param processName
     * @param inputs
     * @param background
     */
    public void submit(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs, boolean background);

    /**
     * 
     * @param executionId
     * @param processName
     * @param inputs
     */
    public void submitChained(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs);

    /**
     * 
     * @param clusterId
     * @param executionId
     * @param value
     */
    public void storeResult(String clusterId, String executionId, Object value);

    public static class ExecutionStatusEx extends ExecutionStatus {

        private String result;

        public ExecutionStatusEx(ExecutionStatus status) {
            super(status.getProcessName(), status.getExecutionId(), status.getPhase(), status
                    .getProgress());
        }

        public ExecutionStatusEx(ExecutionStatus status, String result) {
            this(status);
            this.result = result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        public String getResult() {
            return result;
        }

    }
}
