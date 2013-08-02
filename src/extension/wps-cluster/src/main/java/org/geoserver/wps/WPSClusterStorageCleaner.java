/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.ConfigurationException;

import org.geoserver.config.GeoServerDataDirectory;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ProcessStorage;
import org.geotools.util.logging.Logging;

/**
 * Cleans up the temporary storage directory for WPS
 * 
 * @author Andrea Aime - GeoSolutions
 */
public class WPSClusterStorageCleaner extends WPSStorageCleaner {
    Logger LOGGER = Logging.getLogger(WPSClusterStorageCleaner.class);

    /** The available storages. */
    private List<ProcessStorage> availableStorages;

    /** The list of the executions ID to remove */
    private Map<String, Long> executionDelays;

    /** The cluster ID. */
    private String clusterId;

    /** Is Enabled or not. */
    private Boolean enabled;

    public WPSClusterStorageCleaner(GeoServerDataDirectory dataDirectory) throws IOException,
            ConfigurationException {
        super(dataDirectory);
        this.executionDelays = new HashMap<String, Long>();

        // retrieve all the available process storages
        availableStorages = GeoServerExtensions.extensions(ProcessStorage.class);
    }

    @Override
    public void run() {
        try {
            if (!getStorage().exists())
                return;

            // ok, now scan for existing files there and clean up those
            // that are too old
            long now = System.currentTimeMillis();
            cleanupDirectory(getStorage(), now);
            cleanupDirectory(getWpsOutputStorage(), now);
            cleanupStorages(now);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error occurred while trying to clean up "
                    + "old coverages from temp storage", e);
        }
    }

    /**
     * Recursively cleans up files that are too old
     * 
     * @param directory
     * @param now
     * @throws IOException
     */
    private void cleanupDirectory(File directory, long now) throws IOException {
        for (File f : directory.listFiles()) {
            // skip locked files, someone is downloading them
            if (lockedFiles.contains(f)) {
                continue;
            }
            // cleanup directories recursively
            if (f.isDirectory()) {
                cleanupDirectory(f, now);
                // make sure we delete the directory only if enough time elapsed, since
                // it might have been just created to store some wps outputs
                if (f.list().length == 0 && f.lastModified() > expirationDelay) {
                    f.delete();
                }
            } else {
                if (expirationDelay > 0 && now - f.lastModified() > expirationDelay) {
                    if (f.isFile()) {
                        f.delete();
                    }
                }
            }
        }
    }

    /**
     * Recursively cleans up files that are too old
     * 
     * @param directory
     * @param now
     * @throws IOException
     */
    private void cleanupStorages(long now) throws IOException {
        if (enabled) {
            if (availableStorages != null && availableStorages.size() > 0) {
                for (ProcessStorage storage : availableStorages) {
                    Collection<ExecutionStatus> processesExecutionStauts = storage.getAll();

                    if (processesExecutionStauts != null) {
                        for (ExecutionStatus executionStatus : processesExecutionStauts) {
                            if (executionDelays.get(executionStatus.getExecutionId()) == null || executionDelays.get(executionStatus.getExecutionId()) != null && (expirationDelay > 0
                                        && now
                                                - executionDelays.get(executionStatus
                                                        .getExecutionId()) > expirationDelay)) {
                                    storage.removeStatus(clusterId,
                                            executionStatus.getExecutionId(), true);
                                }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param enabled the enabled to set
     */
    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return the enabled
     */
    public Boolean getEnabled() {
        return enabled;
    }

    /**
     * 
     * @param executionId
     * @param time
     */
    public void scheduleForCleaning(String executionId, long time) {
        if (!executionDelays.containsKey(executionId)) {
            executionDelays.put(executionId, time);
        }
    }
}
