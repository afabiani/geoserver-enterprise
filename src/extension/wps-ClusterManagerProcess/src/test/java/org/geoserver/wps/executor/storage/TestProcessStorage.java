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
 * @author Alessio
 * 
 */
public class TestProcessStorage implements ProcessStorage, ExtensionPriority,
        ApplicationListener<ApplicationEvent> {

    public void onApplicationEvent(ApplicationEvent event) {
        // TODO Auto-generated method stub

    }

    public int getPriority() {
        return ExtensionPriority.HIGHEST;
    }

    public void putStatus(String clusterId, String executionId, ExecutionStatus status) {
        // TODO Auto-generated method stub

    }

    public ExecutionStatus getStatus(String clusterId, String executionId) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<ExecutionStatusEx> getStatus(String executionId) {
        List<ExecutionStatusEx> status = new ArrayList<ExecutionStatusEx>();

        ExecutionStatusEx exStatus = new ExecutionStatusEx(new ExecutionStatus(new NameImpl("gs",
                "TestProcess"), executionId, ProcessState.COMPLETED, 100.0f));
        status.add(exStatus);

        return status;
    }

    public ExecutionStatus removeStatus(String clusterId, String executionId) {
        // TODO Auto-generated method stub
        return null;
    }

    public Collection<ExecutionStatus> getAll() {
        // TODO Auto-generated method stub
        return null;
    }

    public void updatePhase(String clusterId, String executionId, ProcessState phase) {
        // TODO Auto-generated method stub

    }

    public void updateProgress(String clusterId, String executionId, float progress) {
        // TODO Auto-generated method stub

    }

    public Map<String, Object> getOutput(String clusterId, String executionId, long timeout) {
        // TODO Auto-generated method stub
        return null;
    }

    public String getInstance(String executionId) {
        // TODO Auto-generated method stub
        return null;
    }

    public void putOutput(String clusterId, String executionId, ExecutionStatus status) {
        // TODO Auto-generated method stub

    }

    public void putOutput(String clusterId, String executionId, Exception e) {
        // TODO Auto-generated method stub

    }

    public void submit(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs, boolean background) {
        // TODO Auto-generated method stub

    }

    public void submitChained(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs) {
        // TODO Auto-generated method stub

    }

    public void storeResult(String clusterId, String executionId, Object value) {
        // TODO Auto-generated method stub

    }

}
