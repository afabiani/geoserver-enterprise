/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.executor.storage.model.ProcessDescriptor;
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
    public void putStatus( String executionId, ExecutionStatus status, Boolean silently);

    /**
     * Retrieves the status of a process from the storage.
     *
     * @param executionId process id
     * @return the status
     */
    public ExecutionStatus getStatus(String executionId, Boolean silently);


    /**
     * Removes a process from the storage. The last status is returned.
     *
     * 
     * @param executionId process id
     * @return the execution status
     */
    public ExecutionStatus removeProcess( String executionId, Boolean silently);

    /**
     * Gets the status of all executing processes on all the instances of the cluster.
     *
     * @return the all
     */
    public Collection<ProcessDescriptor> getAll(List<ProcessState>status,String clusterID,Date finishedDateTimeLimit);

    /**
     * Updates the phase of a process.
     *
     * @param executionId the execution id
     * @param phase the phase
     */
    public void updatePhase( String executionId, ProcessState phase, Boolean silently);

    /**
     * Updates the progress of a process.
     *
     * 
     * @param executionId the execution id
     * @param progress the progress
     */
    public void updateProgress( String executionId, float progress, Boolean silently);

    /**
     * Retrieves the output of a process, with the given max timeout.
     *
     * 
     * @return the output
     */
    public Map<String, Object> getOutput( String executionId, Boolean silently);

    /**
     * Puts the output of a process on the storage.
     *
     * 
     * @param executionId the execution id
     * @param status the status
     */
    public void putOutput( String executionId, ExecutionStatus status, Boolean silently);

    /**
     * Puts the output error of a process on the storage.
     *
     * 
     * @param executionId the execution id
     * @param e the e
     */
    public void putOutput( String executionId, Exception e, Boolean silently);

    /**
     * Submit.
     *
     * 
     * @param executionId the execution id
     * @param processName the process name
     * @param background the background
     */
    public ProcessDescriptor createOrFindProcess(String clusterId, String executionId, Name processName, boolean background, String email);

    /**
     * Store result.
     *
     * 
     * @param executionId the execution id
     * @param value the value
     */
    public void storeResult( String executionId, Object value, Boolean silently);

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
