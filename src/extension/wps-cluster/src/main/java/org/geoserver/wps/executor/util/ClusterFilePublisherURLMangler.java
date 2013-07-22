/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.util;

import java.io.File;

/**
 * Generinc interface for Cluster File Publishing extensions.
 * 
 * It can be used to plug into GeoServer custom URL manglers for the publishing of processes outputs files.
 * 
 * @author Alessio
 * 
 */
public interface ClusterFilePublisherURLMangler {

    /**
     * 
     * @param file
     * @return
     * @throws Exception
     */
    public String getPublishingURL(File file) throws Exception;

}
