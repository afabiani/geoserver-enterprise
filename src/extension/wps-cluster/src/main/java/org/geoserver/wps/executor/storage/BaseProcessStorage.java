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

import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.executor.storage.model.ProcessDescriptor;

/**
 * Basic Implementation for a {@link ProcessStorage} that actually does nothing
 * @author Simone Giannecchini, GeoSolutions SAS
 *
 */
public class BaseProcessStorage implements ProcessStorage {

    @Override
    public Collection<ProcessDescriptor> getAll(List<ProcessState>status,String clusterID,Date finishedDateTimeLimit) {
        return Collections.emptyList();
    }

    @Override
    public void update(ProcessDescriptor process) {
    }


    @Override
    public boolean remove(ProcessDescriptor process) {
        return false;
    }


    @Override
    public void create(ProcessDescriptor process) {
    }


    @Override
    public ProcessDescriptor findByExecutionId(String executionId, Boolean silently) {
        return null;
    }

    @Override
    public void storeResult(ProcessDescriptor process, Object result) {
        // TODO Auto-generated method stub
        
    }
}
