/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.model;

/**
 * 
 * @author ETj (etj at geo-solutions.it)
 */
public interface Identifiable {

    Long getId();

    void setId(Long id);

    // String getName();
    // void setName(String name);
}
