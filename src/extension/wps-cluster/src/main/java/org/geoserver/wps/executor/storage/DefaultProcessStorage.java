/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.geoserver.platform.ExtensionPriority;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.executor.ProcessStorage;
import org.geoserver.wps.executor.storage.dao.ProcessDescriptorDAO;
import org.geoserver.wps.executor.storage.model.ProcessDescriptor;
import org.geoserver.wps.executor.util.ClusterFilePublisherURLMangler;
import org.geotools.process.ProcessException;
import org.opengis.feature.type.Name;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.googlecode.genericdao.search.Search;
import com.thoughtworks.xstream.XStream;

/**
 * @author Alessio
 * 
 */
public class DefaultProcessStorage implements ProcessStorage, ExtensionPriority,
        ApplicationListener<ApplicationEvent> {

    private XStream marshaller = new XStream();

    private ProcessDescriptorDAO processDescriptorDAO;

    public DefaultProcessStorage(ProcessDescriptorDAO processDescriptorDAO) {
        this.processDescriptorDAO = processDescriptorDAO;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#putStatus(java.lang.String, java.lang.String, org.geoserver.wps.executor.ExecutionStatus)
     */
    @Override
    public void putStatus(String clusterId, String executionId, ExecutionStatus status) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not find any process [" + executionId + "]");
        } else {
            ExecutionStatus newStatus = new ExecutionStatus(status.getProcessName(), executionId,
                    status.getPhase(), status.getProgress());

            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            process.setPhase(status.getPhase());
            process.setProgress(status.getProgress());
            process.setStatus(marshaller.toXML(newStatus));
            processDescriptorDAO.merge(process);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#getStatus(java.lang.String)
     */
    @Override
    public ExecutionStatus getStatus(String clusterId, String executionId) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not retrieve the status of process [" + executionId
                    + "]");
        } else {
            ExecutionStatusEx status = new ExecutionStatusEx(
                    (ExecutionStatus) marshaller.fromXML(processes.get(0).getStatus()), processes
                            .get(0).getResult());

            return status;
        }
    }

    @Override
    public List<ExecutionStatusEx> getStatus(String executionId) {
        List<ExecutionStatusEx> status = new ArrayList<ExecutionStatusEx>();

        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            // throw new ProcessException("Could not retrieve the status of process ["+executionId+"]");
            return null;
        } else {
            for (ProcessDescriptor process : processes) {
                status.add(new ExecutionStatusEx((ExecutionStatus) marshaller.fromXML(process
                        .getStatus()), process.getResult()));
            }

            return status;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#removeStatus(java.lang.String)
     */
    @Override
    public ExecutionStatus removeStatus(String clusterId, String executionId) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#getAll()
     */
    @Override
    public Collection<ExecutionStatus> getAll() {
        return Collections.EMPTY_LIST;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#updatePhase(java.lang.String, org.geoserver.wps.executor.ExecutionStatus.ProcessState)
     */
    @Override
    public void updatePhase(String clusterId, String executionId, ProcessState phase) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not retrieve the phase of process [" + executionId
                    + "]");
        } else {
            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            ExecutionStatus status = (ExecutionStatus) marshaller.fromXML(process.getStatus());
            status.setPhase(phase);
            process.setPhase(phase);
            process.setStatus(marshaller.toXML(status));
            processDescriptorDAO.merge(process);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#updateProgress(java.lang.String, float)
     */
    @Override
    public void updateProgress(String clusterId, String executionId, float progress) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not retrieve the progress of process [" + executionId
                    + "]");
        } else {
            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            ExecutionStatus status = (ExecutionStatus) marshaller.fromXML(process.getStatus());
            status.setProgress(progress);
            process.setProgress(progress);
            process.setStatus(marshaller.toXML(status));
            processDescriptorDAO.merge(process);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#getOutput(java.lang.String, long)
     */
    @Override
    public Map<String, Object> getOutput(String clusterId, String executionId, long timeout) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not find any process [" + executionId + "]");
        } else {
            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            ExecutionStatus status = (ExecutionStatus) marshaller.fromXML(process.getStatus());
            status.setPhase(ProcessState.COMPLETED);
            status.setProgress(100.0f);
            process.setPhase(ProcessState.COMPLETED);
            process.setProgress(100.0f);
            process.setStatus(marshaller.toXML(status));
            processDescriptorDAO.merge(process);
        }

        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#getInstance(java.lang.String)
     */
    @Override
    public String getInstance(String executionId) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not find any process [" + executionId + "]");
        } else {
            return processes.get(0).getClusterId();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#putOutput(java.lang.String, java.lang.String)
     */
    @Override
    public void putOutput(String clusterId, String executionId, ExecutionStatus status) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not find any process [" + executionId + "]");
        } else {
            ExecutionStatus newStatus = new ExecutionStatus(status.getProcessName(), executionId,
                    status.getPhase(), status.getProgress());

            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            process.setPhase(status.getPhase());
            process.setProgress(status.getProgress());
            process.setStatus(marshaller.toXML(newStatus));
            processDescriptorDAO.merge(process);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.geoserver.wps.executor.ProcessStorage#putOutput(java.lang.String, java.lang.Exception)
     */
    @Override
    public void putOutput(String clusterId, String executionId, Exception e) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not find any process [" + executionId + "]");
        } else {
            Writer out = new StringWriter();
            PrintWriter pw = new PrintWriter(out);
            e.printStackTrace(pw);
            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            process.setStatus(pw.toString());
            processDescriptorDAO.merge(process);
        }
    }

    @Override
    public int getPriority() {
        return ExtensionPriority.LOWEST;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        // TODO Auto-generated method stub
    }

    @Override
    public void submit(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs, boolean background) {

        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            ExecutionStatus status = new ExecutionStatus(processName, executionId,
                    ProcessState.QUEUED, 0);

            ProcessDescriptor process = new ProcessDescriptor();
            process.setClusterId(clusterId);
            process.setExecutionId(executionId);
            process.setStatus(marshaller.toXML(status));
            process.setProgress(0.0f);
            process.setPhase(ProcessState.QUEUED);
            processDescriptorDAO.persist(process);
        }
    }

    @Override
    public void submitChained(String clusterId, String executionId, Name processName,
            Map<String, Object> inputs) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            ExecutionStatus status = new ExecutionStatus(processName, executionId,
                    ProcessState.QUEUED, 0);

            ProcessDescriptor process = new ProcessDescriptor();
            process.setClusterId(clusterId);
            process.setExecutionId(executionId);
            process.setStatus(marshaller.toXML(status));
            process.setProgress(0.0f);
            process.setPhase(ProcessState.QUEUED);
            processDescriptorDAO.persist(process);
        }
    }

    @Override
    public void storeResult(String clusterId, String executionId, Object result) {
        Search search = new Search(ProcessDescriptor.class);
        search.addFilterEqual("clusterId", clusterId);
        search.addFilterEqual("executionId", executionId);
        search.addSortDesc("id");
        List<ProcessDescriptor> processes = processDescriptorDAO.search(search);

        if (processes == null || processes.isEmpty()) {
            throw new ProcessException("Could not find any process [" + executionId + "]");
        } else {
            ProcessDescriptor process = processDescriptorDAO.find(processes.get(0).getId());
            if (result instanceof File) {
                List<ClusterFilePublisherURLMangler> filePublishers = getFilePublisherURLManglers();
                if (filePublishers != null && filePublishers.size() > 0)
                    try {
                        process.setResult(filePublishers.get(0).getPublishingURL(((File) result)));
                    } catch (Exception e) {
                        throw new ProcessException(
                                "Could not publish the output file for process [" + executionId
                                        + "]");
                    }
                else
                    process.setResult(((File) result).getAbsolutePath());
            } else {
                process.setResult(result.toString());
            }

            processDescriptorDAO.merge(process);
        }
    }

    public static List<ClusterFilePublisherURLMangler> getFilePublisherURLManglers() {
        return GeoServerExtensions.extensions(ClusterFilePublisherURLMangler.class);
    }

}
