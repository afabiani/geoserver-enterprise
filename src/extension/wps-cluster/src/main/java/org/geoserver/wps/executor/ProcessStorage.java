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
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public interface ProcessStorage {

    /**
     * Creates / Updates a process status.
     * 
     * @param clusterId id of the cluster instance executing the process
     * @param executionId process id
     * @param status current process status
     */
    public void putStatus(String clusterId, String executionId, ExecutionStatus status, Boolean silently);

    /**
     * Retrieves the status of a process from the storage.
     *
     * @param clusterId the cluster id
     * @param executionId process id
     * @return the status
     */
    public ExecutionStatus getStatus(String clusterId, String executionId, Boolean silently);

    /**
     * Retrieves the status of the process from the storage.
     *
     * @param executionId the execution id
     * @return the status
     */
    public List<ExecutionStatusEx> getStatus(String executionId, Boolean silently);

    /**
     * Removes the status of a process from the storage. The last status is returned.
     *
     * @param clusterId the cluster id
     * @param executionId process id
     * @return the execution status
     */
    public ExecutionStatus removeStatus(String clusterId, String executionId, Boolean silently);

    /**
     * Gets the status of all executing processes on all the instances of the cluster.
     *
     * @return the all
     */
    public Collection<ExecutionStatus> getAll();

    /**
     * Updates the phase of a process.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param phase the phase
     */
    public void updatePhase(String clusterId, String executionId, ProcessState phase, Boolean silently);

    /**
     * Updates the progress of a process.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param progress the progress
     */
    public void updateProgress(String clusterId, String executionId, float progress, Boolean silently);

    /**
     * Retrieves the output of a process, with the given max timeout.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param timeout the timeout
     * @return the output
     */
    public Map<String, Object> getOutput(String clusterId, String executionId, long timeout, Boolean silently);

    /**
     * Gets the id of the instance executing a process.
     *
     * @param executionId the execution id
     * @return single instance of ProcessStorage
     */
    public String getInstance(String executionId, Boolean silently);

    /**
     * Puts the output of a process on the storage.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param status the status
     */
    public void putOutput(String clusterId, String executionId, ExecutionStatus status, Boolean silently);

    /**
     * Puts the output error of a process on the storage.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param e the e
     */
    public void putOutput(String clusterId, String executionId, Exception e, Boolean silently);

    /**
     * Submit.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param processName the process name
     * @param inputs the inputs
     * @param background the background
     */
    public void submit(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs, boolean background);

    /**
     * Submit chained.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param processName the process name
     * @param inputs the inputs
     */
    public void submitChained(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs);

    /**
     * Store result.
     *
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param value the value
     */
    public void storeResult(String clusterId, String executionId, Object value, Boolean silently);

    /**
     * The Class ExecutionStatusEx.
     */
    public static class ExecutionStatusEx extends ExecutionStatus {

        /** The result. */
        private String result;

        /**
         * Instantiates a new execution status ex.
         *
         * @param status the status
         */
        public ExecutionStatusEx(ExecutionStatus status) {
            super(status.getProcessName(), status.getExecutionId(), status.getPhase(), status
                    .getProgress());
        }

        /**
         * Instantiates a new execution status ex.
         *
         * @param status the status
         * @param result the result
         */
        public ExecutionStatusEx(ExecutionStatus status, String result) {
            this(status);
            this.result = result;
        }

        /**
         * Sets the result.
         *
         * @param result the new result
         */
        public void setResult(String result) {
            this.result = result;
        }

        /**
         * Gets the result.
         *
         * @return the result
         */
        public String getResult() {
            return result;
        }

    }
}
