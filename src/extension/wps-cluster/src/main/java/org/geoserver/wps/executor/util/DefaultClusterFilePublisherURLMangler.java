/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.util;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.geoserver.config.GeoServer;
import org.geoserver.ows.util.ResponseUtils;
import org.geoserver.platform.ExtensionPriority;
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
    GeoServer geoserver;

    /**
     * Instantiates a new default cluster file publisher url mangler.
     *
     * @param geoserver the geoserver
     */
    public DefaultClusterFilePublisherURLMangler(GeoServer geoserver) {
        this.geoserver = geoserver;
    }

    /**
     * Gets the publishing url.
     *
     * @param file the file
     * @return the publishing url
     * @throws Exception the exception
     */
    @Override
    public String getPublishingURL(File file) throws Exception {
        File gsTempFolder = GeoserverDataDirectory.findCreateConfigDir("temp");
        FileUtils.copyFileToDirectory(file, gsTempFolder, true);

        String url = geoserver.getGlobal().getSettings().getProxyBaseUrl();
        url = url == null ? "/" : url;
        return ResponseUtils.appendPath(url, "/temp/" + FilenameUtils.getName(file.getName()));
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
