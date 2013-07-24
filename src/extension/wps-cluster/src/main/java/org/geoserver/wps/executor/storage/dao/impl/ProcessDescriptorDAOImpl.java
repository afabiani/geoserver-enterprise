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
 * Public implementation of the ProcessDescriptorDAO interface.
 *
 * @author Emanuele Tajariol (etj at geo-solutions.it)
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@Transactional(value = "processStorageTransactionManager")
public class ProcessDescriptorDAOImpl extends BaseDAO<ProcessDescriptor, Long> implements
        ProcessDescriptorDAO {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logger.getLogger(ProcessDescriptorDAOImpl.class);

    /**
     * Persist.
     *
     * @param entities the entities
     */
    @Override
    public void persist(ProcessDescriptor... entities) {
        super.persist(entities);
    }

    /**
     * Find all.
     *
     * @return the list
     */
    @Override
    public List<ProcessDescriptor> findAll() {
        return super.findAll();
    }

    /**
     * Search.
     *
     * @param search the search
     * @return the list
     */
    @Override
    public List<ProcessDescriptor> search(ISearch search) {
        return super.search(search);
    }

    /**
     * Merge.
     *
     * @param entity the entity
     * @return the process descriptor
     */
    @Override
    public ProcessDescriptor merge(ProcessDescriptor entity) {
        return super.merge(entity);
    }

    /**
     * Removes the.
     *
     * @param entity the entity
     * @return true, if successful
     */
    @Override
    public boolean remove(ProcessDescriptor entity) {
        return super.remove(entity);
    }

    /**
     * Removes the by id.
     *
     * @param id the id
     * @return true, if successful
     */
    @Override
    public boolean removeById(Long id) {
        return super.removeById(id);
    }

}
