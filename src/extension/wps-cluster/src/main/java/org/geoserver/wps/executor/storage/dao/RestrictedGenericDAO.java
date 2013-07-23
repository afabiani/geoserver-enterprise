/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.dao;

import java.util.List;

import com.googlecode.genericdao.search.ISearch;

/**
 * Public interface to define a restricted set of operation wrt to ones defined in GenericDAO. This may be useful if some constraints are implemented
 * in the DAO, so that fewer point of access are allowed.
 *
 * @param <ENTITY> the generic type
 * @author Emanuele Tajariol (etj at geo-solutions.it)
 */

public interface RestrictedGenericDAO<ENTITY> /* extends GenericDAO<ENTITY, Long> */{

    /**
     * Find all.
     *
     * @return the list
     */
    public List<ENTITY> findAll();

    /**
     * Find.
     *
     * @param id the id
     * @return the entity
     */
    public ENTITY find(Long id);

    /**
     * Persist.
     *
     * @param entities the entities
     */
    public void persist(ENTITY... entities);

    /**
     * Merge.
     *
     * @param entity the entity
     * @return the entity
     */
    public ENTITY merge(ENTITY entity);

    /**
     * Removes the.
     *
     * @param entity the entity
     * @return true, if successful
     */
    public boolean remove(ENTITY entity);

    /**
     * Removes the by id.
     *
     * @param id the id
     * @return true, if successful
     */
    public boolean removeById(Long id);

    /**
     * Search.
     *
     * @param search the search
     * @return the list
     */
    public List<ENTITY> search(ISearch search);

    /**
     * Count.
     *
     * @param search the search
     * @return the int
     */
    public int count(ISearch search);
}
