/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2011, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geoserver.wps.executor.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.executor.storage.model.ProcessDescriptor;
import org.opengis.feature.type.Name;

/**
 * Basic Implementation for a {@link ProcessStorage} that actually does nothing
 * @author Simone Giannecchini, GeoSolutions SAS
 *
 */
public class BaseProcessStorage implements ProcessStorage {

    @Override
    public void putStatus(String executionId, ExecutionStatus status,
            Boolean silently) {

    }

    @Override
    public ExecutionStatus getStatus( String executionId, Boolean silently) {
        return null;
    }

    @Override
    public ExecutionStatus removeProcess(String executionId, Boolean silently) {
        return null;
    }

    @Override
    public Collection<ProcessDescriptor> getAll(List<ProcessState>status,String clusterID,Date finishedDateTimeLimit) {
        return Collections.emptyList();
    }

    @Override
    public void updatePhase(String executionId, ProcessState phase,
            Boolean silently) {
    }

    @Override
    public void updateProgress(String executionId, float progress,
            Boolean silently) {
    }

    @Override
    public Map<String, Object> getOutput(String executionId, 
            Boolean silently) {
        return Collections.emptyMap();
    }

    @Override
    public void putOutput(String executionId, ExecutionStatus status,
            Boolean silently) {
    }

    @Override
    public void putOutput(String executionId, Exception e, Boolean silently) {
    }

    @Override
    public ProcessDescriptor createOrFindProcess(String clusterId, String executionId, Name processName, boolean background,String email) {
        return null;
    }

    @Override
    public void storeResult(String executionId, Object value, Boolean silently) {

    }
}
