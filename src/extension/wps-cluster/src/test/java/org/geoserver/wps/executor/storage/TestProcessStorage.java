/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.geoserver.platform.ExtensionPriority;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.executor.ProcessStorage;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.Name;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * The Class TestProcessStorage.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class TestProcessStorage implements ProcessStorage, ExtensionPriority,
        ApplicationListener<ApplicationEvent> {

    /**
     * On application event.
     * 
     * @param event the event
     */
    public void onApplicationEvent(ApplicationEvent event) {
        // TODO Auto-generated method stub

    }

    /**
     * Gets the priority.
     * 
     * @return the priority
     */
    public int getPriority() {
        return ExtensionPriority.HIGHEST;
    }

    /**
     * Put status.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param status the status
     */
    public void putStatus(String clusterId, String executionId, ExecutionStatus status,
            Boolean silently) {
        // TODO Auto-generated method stub

    }

    /**
     * Gets the status.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @return the status
     */
    public ExecutionStatus getStatus(String clusterId, String executionId, Boolean silently) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Gets the status.
     * 
     * @param executionId the execution id
     * @return the status
     */
    public List<ExecutionStatusEx> getStatus(String executionId, Boolean silently) {
        List<ExecutionStatusEx> status = new ArrayList<ExecutionStatusEx>();

        ExecutionStatusEx exStatus = new ExecutionStatusEx(new ExecutionStatus(new NameImpl("gs",
                "TestProcess"), executionId, ProcessState.COMPLETED, 100.0f));
        status.add(exStatus);

        return status;
    }

    /**
     * Removes the status.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @return the execution status
     */
    public ExecutionStatus removeStatus(String clusterId, String executionId, Boolean silently) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Gets the all.
     * 
     * @return the all
     */
    public Collection<ExecutionStatus> getAll() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Update phase.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param phase the phase
     */
    public void updatePhase(String clusterId, String executionId, ProcessState phase,
            Boolean silently) {
        // TODO Auto-generated method stub

    }

    /**
     * Update progress.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param progress the progress
     */
    public void updateProgress(String clusterId, String executionId, float progress,
            Boolean silently) {
        // TODO Auto-generated method stub

    }

    /**
     * Gets the output.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param timeout the timeout
     * @return the output
     */
    public Map<String, Object> getOutput(String clusterId, String executionId, long timeout,
            Boolean silently) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Gets the single instance of TestProcessStorage.
     * 
     * @param executionId the execution id
     * @return single instance of TestProcessStorage
     */
    public String getInstance(String executionId, Boolean silently) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Put output.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param status the status
     */
    public void putOutput(String clusterId, String executionId, ExecutionStatus status,
            Boolean silently) {
        // TODO Auto-generated method stub

    }

    /**
     * Put output.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param e the e
     */
    public void putOutput(String clusterId, String executionId, Exception e, Boolean silently) {
        // TODO Auto-generated method stub

    }

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
            Map<String, Object> inputs, boolean background) {
        // TODO Auto-generated method stub

    }

    /**
     * Submit chained.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param processName the process name
     * @param inputs the inputs
     */
    public void submitChained(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs) {
        // TODO Auto-generated method stub

    }

    /**
     * Store result.
     * 
     * @param clusterId the cluster id
     * @param executionId the execution id
     * @param value the value
     */
    public void storeResult(String clusterId, String executionId, Object value, Boolean silently) {
        // TODO Auto-generated method stub

    }

}
