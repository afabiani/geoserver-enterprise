/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.util;

import java.io.File;

import org.geoserver.ows.URLMangler.URLType;
import org.geoserver.ows.util.ResponseUtils;
import org.geoserver.platform.ExtensionPriority;
import org.geoserver.wps.resource.WPSResourceManager;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;

/**
 * The Class DefaultClusterFilePublisherURLMangler.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class DefaultClusterFilePublisherURLMangler implements ClusterFilePublisherURLMangler,
        ExtensionPriority, ApplicationListener<ApplicationEvent> {

    /** The geoserver. */
    WPSResourceManager wpsResourceManager;

    /**
     * Instantiates a new default cluster file publisher url mangler.
     * 
     * @param geoserver the geoserver
     */
    public DefaultClusterFilePublisherURLMangler(WPSResourceManager wpsResourceManager) {
        this.wpsResourceManager = wpsResourceManager;
    }

    /**
     * Gets the publishing url.
     * 
     * @param file the file
     * @return the publishing url
     * @throws Exception the exception
     */
    @Override
    public String getPublishingURL(File file, String baseURL) throws Exception {

        // relativize to temp dir directory
        String path =  GeoserverDataDirectory.getGeoserverDataDirectory().toURI().relativize(file.toURI()).getPath();
        return ResponseUtils.buildURL(baseURL, path, null, URLType.RESOURCE);
    }

    /**
     * On application event.
     * 
     * @param event the event
     */
    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        // TODO Auto-generated method stub
    }

    /**
     * Gets the priority.
     * 
     * @return the priority
     */
    @Override
    public int getPriority() {
        return ExtensionPriority.LOWEST;
    }

}
