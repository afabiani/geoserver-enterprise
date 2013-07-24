/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.model;

/**
 * The Interface Identifiable.
 *
 * @author ETj (etj at geo-solutions.it)
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public interface Identifiable {

    /**
     * Gets the id.
     *
     * @return the id
     */
    Long getId();

    /**
     * Sets the id.
     *
     * @param id the new id
     */
    void setId(Long id);

    // String getName();
    // void setName(String name);
}
