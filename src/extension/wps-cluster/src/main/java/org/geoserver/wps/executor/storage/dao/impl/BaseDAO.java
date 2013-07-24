/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.dao.impl;

import com.googlecode.genericdao.dao.jpa.GenericDAOImpl;
import com.googlecode.genericdao.search.jpa.JPASearchProcessor;

import java.io.Serializable;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

//import com.trg.dao.jpa.GenericDAOImpl;
//import com.trg.search.jpa.JPASearchProcessor;

import org.springframework.stereotype.Repository;

/**
 * The base DAO furnish a set of methods usually used.
 *
 * @param <T> the generic type
 * @param <ID> the generic type
 * @author Tobia Di Pisa (tobia.dipisa@geo-solutions.it)
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@Repository(value = "processStorage")
public class BaseDAO<T, ID extends Serializable> extends GenericDAOImpl<T, ID> {

    /** The entity manager. */
    @PersistenceContext(unitName = "processStorageEntityManagerFactory")
    private EntityManager entityManager;

    /**
     * EntityManager setting.
     *
     * @param entityManager the entity manager to set
     */
    @Override
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
        super.setEntityManager(this.entityManager);
    }

    /**
     * JPASearchProcessor setting.
     *
     * @param searchProcessor the search processor to set
     */
    @Override
    public void setSearchProcessor(JPASearchProcessor searchProcessor) {
        super.setSearchProcessor(searchProcessor);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.trg.dao.jpa.JPABaseDAO#em()
     */
    /**
     * Em.
     *
     * @return the entity manager
     */
    @Override
    public EntityManager em() {
        return this.entityManager;
    }
}
