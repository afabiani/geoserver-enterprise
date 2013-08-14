/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.geoserver.catalog.Catalog;
import org.geoserver.config.GeoServer;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ProcessStorage;
import org.geoserver.wps.executor.ProcessStorage.ExecutionStatusEx;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.util.logging.Logging;
import org.opengis.util.ProgressListener;

/**
 * The Class ClusterManagerProcess.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@DescribeProcess(title = "Enterprise Cluster-Manager Process", description = "Allows to retrieve the Execution Status of the Cluster running processes.")
public class ClusterManagerProcess implements GSProcess {

    /** The Constant LOGGER. */
    protected static final Logger LOGGER = Logging.getLogger(ClusterManagerProcess.class);

    /** The geo server. */
    protected GeoServer geoServer;

    /** The catalog. */
    protected Catalog catalog;

    /** The available storages. */
    private List<ProcessStorage> availableStorages;

    /**
     * Instantiates a new cluster manager process.
     * 
     * @param geoServer the geo server
     */
    public ClusterManagerProcess(GeoServer geoServer) {
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();

        availableStorages = GeoServerExtensions.extensions(ProcessStorage.class);
    }

    /**
     * Execute.
     * 
     * @param executionId the execution id
     * @param progressListener the progress listener
     * @return the list
     * @throws ProcessException the process exception
     */
    @DescribeResult(name = "result", description = "Zipped output files to download")
    public List<ExecutionStatus> execute(
            @DescribeParameter(name = "executionId", min = 1, description = "The requested WPS ExecutionId") String executionId,
            ProgressListener progressListener) throws ProcessException {

        Throwable cause = null;

        if (availableStorages == null || availableStorages.size() == 0) {
            cause = new Exception("No available Process Storage registered on GeoServer!");
        } else {
            List<ExecutionStatus> processesStatus = new ArrayList<ExecutionStatus>();

            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    List<ExecutionStatusEx> status = storage.getStatus(executionId, true);

                    if (status != null && status.size() > 0) {
                        for (ExecutionStatus exStatus : status) {
                            boolean canUpdate = true;

                            for (ExecutionStatus storedStatus : processesStatus) {
                                if (storedStatus.getExecutionId().equals(exStatus.getExecutionId())
                                        && exStatus.getProgress() < storedStatus.getProgress())
                                    canUpdate = false;
                            }

                            if (exStatus.getProcessName().getLocalPart()
                                    .equalsIgnoreCase("TestProcess")
                                    || !exStatus.getExecutionId().equals(executionId))
                                canUpdate = false;

                            if (canUpdate) {
                                for (ExecutionStatus storedStatus : processesStatus) {
                                    if (exStatus.getProcessName().getLocalPart()
                                            .equalsIgnoreCase("TestProcess")
                                            || !storedStatus.getExecutionId().equals(
                                                    exStatus.getExecutionId())
                                            || (storedStatus.getExecutionId().equals(
                                                    exStatus.getExecutionId()) && exStatus
                                                    .getProgress() < storedStatus.getProgress()))
                                        canUpdate = false;
                                }
                            }
                            if (canUpdate) {
                                processesStatus.add(exStatus);
                            }
                        }
                    }
                }
            }

            return processesStatus;
        }

        throw new ProcessException("Could not complete the Download Process", cause);
    }

}