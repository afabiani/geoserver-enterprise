/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.ConfigurationException;

import org.geoserver.config.GeoServerDataDirectory;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wps.executor.ClusterProcessManager;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.executor.ProcessStorage;
import org.geotools.util.logging.Logging;

/**
 * Cleans up the temporary storage directory for WPS
 * 
 * @author Andrea Aime - GeoSolutions
 */
public class WPSClusterStorageCleaner extends TimerTask {
    Logger LOGGER = Logging.getLogger(WPSClusterStorageCleaner.class);

    /** the local/shared WPS storage. */
    private File storage;

    /** The ClusterProcessManager. */
    ClusterProcessManager clusteredProcessManager;
    
    /** The available storages. */
    private List<ProcessStorage> availableStorages;

    /** The cluster ID. */
    private String clusterId;

    public WPSClusterStorageCleaner(ClusterProcessManager clusteredProcessManager,
            GeoServerDataDirectory dataDirectory) throws IOException, ConfigurationException {
        
        this.clusteredProcessManager = clusteredProcessManager;
        this.clusterId = clusteredProcessManager.getClusterId();
        
        // retrieve all the available process storages
        availableStorages = GeoServerExtensions.extensions(ProcessStorage.class);

        // get the temporary storage for WPS
        try {
            String wpsOutputStorage = GeoServerExtensions.getProperty("WPS_OUTPUT_STORAGE");
            File temp = null;
            if (wpsOutputStorage == null || !new File(wpsOutputStorage).exists())
                temp = dataDirectory.findOrCreateDataDir("temp/wps");
            else {
                temp = new File(wpsOutputStorage, "wps");
            }
            storage = temp;
        } catch (Exception e) {
            throw new IOException("Could not find the temporary storage directory for WPS");
        }
    }

    @Override
    public void run() {
        try {
            if (!storage.exists())
                return;

            // ok, now scan for existing files there and clean up those
            // that are too old
            long now = System.currentTimeMillis();
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
    private void cleanupStorages(long now) throws IOException {
        if (availableStorages != null && availableStorages.size() > 0) {
            for (ProcessStorage storage : availableStorages) {
                Collection<ExecutionStatus> processesExecutionStauts = storage.getAll();

                for (ExecutionStatus executionStatus : processesExecutionStauts) {
                    if (executionStatus.getPhase().equals(ProcessState.CANCELLED)
                            || executionStatus.getPhase().equals(ProcessState.COMPLETED)
                            || executionStatus.getPhase().equals(ProcessState.FAILED)
                            || executionStatus.getProgress() == 100.0f) {
                        storage.removeStatus(clusterId, executionStatus.getExecutionId());
                    }
                }
            }
        }
    }

    /**
     * Returns the storage directory for WPS
     * 
     * @return
     */
    public File getStorage() {
        return storage;
    }

}
