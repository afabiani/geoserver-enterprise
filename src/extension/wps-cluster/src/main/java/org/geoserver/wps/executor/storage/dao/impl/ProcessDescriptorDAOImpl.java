/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.dao.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.geoserver.wps.executor.storage.dao.ProcessDescriptorDAO;
import org.geoserver.wps.executor.storage.model.ProcessDescriptor;
import org.springframework.transaction.annotation.Transactional;

import com.googlecode.genericdao.search.ISearch;

/**
 * Public implementation of the ProcessDescriptorDAO interface
 * 
 * @author Emanuele Tajariol (etj at geo-solutions.it)
 */
@Transactional(value = "processStorageTransactionManager")
public class ProcessDescriptorDAOImpl extends BaseDAO<ProcessDescriptor, Long> implements
        ProcessDescriptorDAO {

    private static final Logger LOGGER = Logger.getLogger(ProcessDescriptorDAOImpl.class);

    @Override
    public void persist(ProcessDescriptor... entities) {
        super.persist(entities);
    }

    @Override
    public List<ProcessDescriptor> findAll() {
        return super.findAll();
    }

    @Override
    public List<ProcessDescriptor> search(ISearch search) {
        return super.search(search);
    }

    @Override
    public ProcessDescriptor merge(ProcessDescriptor entity) {
        return super.merge(entity);
    }

    @Override
    public boolean remove(ProcessDescriptor entity) {
        return super.remove(entity);
    }

    @Override
    public boolean removeById(Long id) {
        return super.removeById(id);
    }

}
