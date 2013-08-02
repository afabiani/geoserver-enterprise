/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.geoserver.ows.Ows11Util;
import org.geoserver.platform.ExtensionPriority;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wps.WPSClusterStorageCleaner;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.resource.WPSResourceManager;
import org.geotools.process.Process;
import org.geotools.process.ProcessException;
import org.geotools.process.ProcessFactory;
import org.geotools.process.Processors;
import org.opengis.feature.type.Name;

/**
 * Alternative implementation of ProcessManager, using a storage (ProcessStorage) to share process status between the instances of a cluster.
 * 
 * @author "Mauro Bartolomeoli - mauro.bartolomeoli@geo-solutions.it"
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class ClusterProcessManager extends DefaultProcessManager {

    /** The cluster id. */
    private String clusterId;

    /** The local processes. */
    private Map<String, ExecutionStatus> localProcesses;

    /** The available storages. */
    private List<ProcessStorage> availableStorages;

    /** The WPS Storage Cleaner. */
    private WPSClusterStorageCleaner cleaner;

    /** The list of excluded proces names. */
    private List<String> processNamesEsclusionList;

    /**
     * Submit chained.
     * 
     * @param executionId the execution id
     * @param processName the process name
     * @param inputs the inputs
     * @return the map
     * @throws ProcessException the process exception
     */
    @Override
    public Map<String, Object> submitChained(String executionId, Name processName,
            Map<String, Object> inputs) throws ProcessException {
        // straight execution, no thread pooling, we're already running in the parent process thread
        ClusterProcessListener listener = new ClusterProcessListener(new ExecutionStatus(
                processName, executionId, ProcessState.RUNNING, 0));
        ProcessFactory pf = Processors.createProcessFactory(processName);
        if (pf == null) {
            throw new WPSException("No such process: " + processName);
        }

        if (this.cleaner != null) {
            long now = System.currentTimeMillis();
            this.cleaner.scheduleForCleaning(executionId, now);
        }

        // execute the process in the same thread as the caller
        Process p = pf.create(processName);
        Map<String, Object> result = p.execute(inputs, listener);
        if (listener.exception != null) {
            if (!(processNamesEsclusionList.contains(processName.getLocalPart()))) {
                if (availableStorages != null && availableStorages.size() > 0) {
                    for (ProcessStorage storage : availableStorages) {
                        String clusterId = storage.getInstance(executionId.toString(), false);
                        storage.putStatus(clusterId, executionId, new ExecutionStatus(processName,
                                executionId, ProcessState.FAILED, 100), false);
                        storage.storeResult(clusterId, executionId,
                                listener.exception.getMessage(), false);
                    }
                }
            }
            throw new ProcessException("Process failed: " + listener.exception.getMessage(),
                    listener.exception);
        }
        return result;
    }

    /**
     * Submit.
     * 
     * @param executionId the execution id
     * @param processName the process name
     * @param inputs the inputs
     * @param background the background
     * @throws ProcessException the process exception
     */
    @Override
    public void submit(String executionId, Name processName, Map<String, Object> inputs,
            boolean background) throws ProcessException {
        ExecutionStatusEx status = createExecutionStatus(processName, executionId, background);
        ClusterProcessListener listener = new ClusterProcessListener(status);
        status.listener = listener;
        ClusterProcessCallable callable = new ClusterProcessCallable(inputs, status, listener);
        Future<Map<String, Object>> future;
        if (background) {
            future = asynchService.submit(callable);
        } else {
            future = synchService.submit(callable);
        }
        status.future = future;
        executions.put(executionId, status);
    }

    /**
     * The Class ClusterProcessCallable.
     */
    class ClusterProcessCallable implements Callable<Map<String, Object>> {

        /** The inputs. */
        Map<String, Object> inputs;

        /** The status. */
        ExecutionStatus status;

        /** The listener. */
        ProcessListener listener;

        /**
         * Instantiates a new cluster process callable.
         * 
         * @param inputs the inputs
         * @param status the status
         * @param listener the listener
         */
        public ClusterProcessCallable(Map<String, Object> inputs, ExecutionStatus status,
                ProcessListener listener) {
            this.inputs = inputs;
            this.status = status;
            this.listener = listener;
        }

        /**
         * Call.
         * 
         * @return the map
         * @throws Exception the exception
         */
        @Override
        public Map<String, Object> call() throws Exception {
            resourceManager.setCurrentExecutionId(status.getExecutionId());
            status.setPhase(ProcessState.RUNNING);
            Name processName = status.getProcessName();
            ProcessFactory pf = Processors.createProcessFactory(processName);
            if (pf == null) {
                throw new WPSException("No such process: " + processName);
            }

            // execute the process
            Map<String, Object> result = null;
            Process p = null;
            try {
                p = pf.create(processName);

                if (p != null && !(processNamesEsclusionList.contains(processName.getLocalPart()))) {
                    if (availableStorages != null && availableStorages.size() > 0) {
                        for (ProcessStorage storage : availableStorages) {
                            storage.submit(clusterId, status.getExecutionId(), processName, inputs,
                                    ((ClusterExecutionStatus) status).isBackground());
                        }
                    }
                }

                result = p.execute(inputs, listener);
                String executionId = status.executionId;

                if (cleaner != null) {
                    long now = System.currentTimeMillis();
                    cleaner.scheduleForCleaning(executionId, now);
                }

                if (listener.exception != null) {
                    status.setPhase(ProcessState.FAILED);

                    if (p != null
                            && !(processNamesEsclusionList.contains(processName.getLocalPart()))) {
                        if (availableStorages != null && availableStorages.size() > 0) {
                            for (ProcessStorage storage : availableStorages) {
                                String clusterId = storage.getInstance(executionId, false);
                                storage.putStatus(clusterId, executionId, new ExecutionStatus(
                                        processName, executionId, ProcessState.FAILED, 100), false);
                                storage.storeResult(
                                        clusterId,
                                        executionId,
                                        listener.exception.getMessage()
                                                + ": "
                                                + (listener.exception.getCause() != null ? listener.exception
                                                        .getCause().getMessage() : ""), false);
                            }
                        }
                    }
                    throw new WPSException("Process failed: " + listener.exception.getMessage(),
                            listener.exception);
                } else if (p != null
                        && !(processNamesEsclusionList.contains(processName.getLocalPart()))) {
                    if (availableStorages != null && availableStorages.size() > 0) {
                        for (ProcessStorage storage : availableStorages) {
                            String clusterId = storage.getInstance(executionId, false);
                            storage.putStatus(clusterId, executionId, new ExecutionStatus(
                                    processName, executionId, ProcessState.COMPLETED, 100), false);

                            for (Entry<String, Object> entry : result.entrySet()) {
                                if (entry.getKey().equalsIgnoreCase("result"))
                                    storage.storeResult(clusterId, executionId, entry.getValue(),
                                            false);
                            }
                        }
                    }

                }
                return result;
            } catch (Exception e) {
                if (status.getPhase() != ProcessState.FAILED) {
                    String executionId = status.executionId;
                    status.setPhase(ProcessState.FAILED);

                    if (p != null
                            && !(processNamesEsclusionList.contains(processName.getLocalPart()))) {
                        if (availableStorages != null && availableStorages.size() > 0) {
                            for (ProcessStorage storage : availableStorages) {
                                String clusterId = storage.getInstance(executionId, false);
                                storage.putStatus(clusterId, executionId, new ExecutionStatus(
                                        processName, executionId, ProcessState.FAILED, 100), false);
                                storage.storeResult(clusterId, executionId, e.getMessage() + ": "
                                        + (e.getCause() != null ? e.getCause().getMessage() : ""),
                                        false);
                            }
                        }
                    }
                    throw new WPSException("Process failed: " + e.getMessage(), e);
                } else
                    throw e;
            } finally {
                // update status unless cancelled
                if (status.getPhase() == ProcessState.RUNNING) {
                    status.setPhase(ProcessState.COMPLETED);
                }
            }
        }

    }

    /**
     * The Class ExecutionsManager.
     */
    class ExecutionsManager extends AbstractMap<String, ExecutionStatus> {

        /**
         * Entry set.
         * 
         * @return the sets the
         */
        @Override
        public Set<java.util.Map.Entry<String, ExecutionStatus>> entrySet() {
            // implementation not needed by ProcessManager
            return null;
        }

        /**
         * Gets the.
         * 
         * @param executionId the execution id
         * @return the execution status
         */
        @Override
        public ExecutionStatus get(Object executionId) {
            if (executionId == null) {
                return null;
            }
            ExecutionStatus status = null;
            if (localProcesses.containsKey(executionId)) {
                status = localProcesses.get(executionId);
            }

            if (status != null)
                if (availableStorages != null && availableStorages.size() > 0) {
                    for (ProcessStorage storage : availableStorages) {
                        if (storage.getInstance(executionId.toString(), true) != null) {
                            String clusterId = storage.getInstance(executionId.toString(), true);
                            // return new ClusterExecutionStatus(clusterId, storage.getStatus(clusterId, executionId.toString()));
                            storage.putStatus(clusterId, executionId.toString(), status, true);
                        }
                    }
                }

            return status;
        }

        /**
         * Put.
         * 
         * @param executionId the execution id
         * @param status the status
         * @return the execution status
         */
        @Override
        public ExecutionStatus put(String executionId, ExecutionStatus status) {
            localProcesses.put(executionId, status);
            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    storage.putStatus(clusterId, executionId, status, true);
                }
            }
            return status;
        }

        /**
         * Removes the.
         * 
         * @param executionId the execution id
         * @return the execution status
         */
        @Override
        public ExecutionStatus remove(Object executionId) {
            if (executionId == null) {
                return null;
            }
            ExecutionStatus status = null;
            if (localProcesses.containsKey(executionId)) {
                if (availableStorages != null && availableStorages.size() > 0) {
                    for (ProcessStorage storage : availableStorages) {
                        status = storage.removeStatus(clusterId, executionId.toString(), true);
                    }
                }
                return localProcesses.remove(executionId);
            }
            return status;
        }

        /**
         * Values.
         * 
         * @return the collection
         */
        @Override
        public Collection<ExecutionStatus> values() {
            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    return storage.getAll();
                }
            }

            return null;
        }
    }

    /**
     * The Class ClusterExecutionStatus.
     */
    class ClusterExecutionStatus extends ExecutionStatusEx {

        /** The local process. */
        private boolean localProcess;

        /** The cluster id. */
        private String clusterId;

        /** The background. */
        private boolean background;

        /**
         * Instantiates a new cluster execution status.
         * 
         * @param processName the process name
         * @param clusterId the cluster id
         * @param executionId the execution id
         */
        public ClusterExecutionStatus(Name processName, String clusterId, String executionId,
                boolean background) {
            super(processName, executionId);
            this.clusterId = clusterId;
            this.localProcess = this.clusterId == ClusterProcessManager.this.clusterId;
            this.background = background;
        }

        /**
         * Instantiates a new cluster execution status.
         * 
         * @param clusterId the cluster id
         * @param status the status
         */
        public ClusterExecutionStatus(String clusterId, ExecutionStatus status) {
            this(status.getProcessName(), clusterId, status.getExecutionId(), false);
            this.phase = status.getPhase();
            this.progress = status.getProgress();
        }

        /**
         * Checks if is local process.
         * 
         * @return true, if is local process
         */
        public boolean isLocalProcess() {
            return localProcess;
        }

        /**
         * Gets the cluster id.
         * 
         * @return the cluster id
         */
        public String getClusterId() {
            return clusterId;
        }

        /**
         * Sets the phase.
         * 
         * @param phase the new phase
         */
        @Override
        public void setPhase(ProcessState phase) {
            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    storage.updatePhase(clusterId, executionId, phase, true);
                    if (phase == ProcessState.COMPLETED && localProcess) {
                        try {
                            ExecutionStatus newStatus = new ExecutionStatus(
                                    Ows11Util.name(clusterId), executionId, phase, 100.0f);
                            storage.putOutput(clusterId, executionId, newStatus, true);
                        } catch (Exception e) {
                            storage.putOutput(clusterId, executionId, e, true);
                        }
                    }
                }
            }
        }

        /**
         * Gets the output.
         * 
         * @param timeout the timeout
         * @return the output
         * @throws Exception the exception
         */
        @Override
        public Map<String, Object> getOutput(long timeout) throws Exception {
            Map<String, Object> output = null;

            if (localProcess) {
                output = super.getOutput(timeout);
            }

            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    Map<String, Object> psOutput = storage.getOutput(clusterId, executionId,
                            timeout, true);

                    if (output != null) {
                        // Special case when output contains a file
                        for (Entry<String, Object> entry : output.entrySet()) {
                            if (entry.getKey().equalsIgnoreCase("result"))
                                storage.storeResult(clusterId, executionId, entry.getValue(), true);
                        }
                    } else if (psOutput != null) {
                        output = psOutput;
                    }
                }
            }

            return output;
        }

        /**
         * Gets the status.
         * 
         * @return the status
         */
        @Override
        public ExecutionStatus getStatus() {
            return this;
        }

        /**
         * Sets the progress.
         * 
         * @param progress the new progress
         */
        @Override
        public void setProgress(float progress) {
            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    storage.updateProgress(clusterId, executionId, progress, true);
                }
            }
        }

        /**
         * @param background the background to set
         */
        public void setBackground(boolean background) {
            this.background = background;
        }

        /**
         * @return the background
         */
        public boolean isBackground() {
            return background;
        }
    }

    /**
     * Instantiates a new cluster process manager.
     * 
     * @param resourceManager the resource manager
     */
    public ClusterProcessManager(WPSResourceManager resourceManager) {
        super(resourceManager);

        availableStorages = GeoServerExtensions.extensions(ProcessStorage.class);
        processNamesEsclusionList = new ArrayList<String>();

        if (GeoServerExtensions.getProperty("CLUSTER_PROCESS_MANAGER_ID") != null) {
            clusterId = GeoServerExtensions.getProperty("CLUSTER_PROCESS_MANAGER_ID");
        } else {
            clusterId = UUID.randomUUID().toString();
        }
    }

    /**
     * Instantiates a new cluster process manager using a list of excluded processes.
     * 
     * @param resourceManager
     * @param clusterId
     * @param localProcesses
     * @param availableStorages
     */
    public ClusterProcessManager(WPSResourceManager resourceManager,
            WPSClusterStorageCleaner cleaner, List<String> processNamesEsclusionList) {
        this(resourceManager);
        this.cleaner = cleaner;
        this.processNamesEsclusionList = processNamesEsclusionList;
    }

    /**
     * Gets the cluster id.
     * 
     * @return the cluster id
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Creates the execution status.
     * 
     * @param processName the process name
     * @param executionId the execution id
     * @param background
     * @return the execution status ex
     */
    protected ExecutionStatusEx createExecutionStatus(Name processName, String executionId,
            boolean background) {
        return new ClusterExecutionStatus(processName, clusterId, executionId, background);
    }

    /**
     * Gets the priority.
     * 
     * @return the priority
     */
    @Override
    public int getPriority() {
        return ExtensionPriority.HIGHEST;
    }

    /**
     * Listens to the process progress and allows to cancel it.
     * 
     * @author Andrea Aime - GeoSolutions
     */
    public class ClusterProcessListener extends ProcessListener {

        /**
         * Instantiates a new cluster process listener.
         * 
         * @param status the status
         */
        public ClusterProcessListener(ExecutionStatus status) {
            super(status);
        }

        /**
         * Sets the status.
         * 
         * @param status the status to set
         */
        public void setStatus(ExecutionStatus status) {
            this.status = status;
        }

        /**
         * Gets the status.
         * 
         * @return the status
         */
        public ExecutionStatus getStatus() {
            return status;
        }

    }

}