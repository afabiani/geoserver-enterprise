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

public class DefaultClusterFilePublisherURLMangler implements ClusterFilePublisherURLMangler,
        ExtensionPriority, ApplicationListener<ApplicationEvent> {

    GeoServer geoserver;

    public DefaultClusterFilePublisherURLMangler(GeoServer geoserver) {
        this.geoserver = geoserver;
    }

    @Override
    public String getPublishingURL(File file) throws Exception {
        File gsTempFolder = GeoserverDataDirectory.findCreateConfigDir("temp");
        FileUtils.copyFileToDirectory(file, gsTempFolder, true);

        String url = geoserver.getGlobal().getSettings().getProxyBaseUrl();
        url = url == null ? "/" : url;
        return ResponseUtils.appendPath(url, "/temp/" + FilenameUtils.getName(file.getName()));
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        // TODO Auto-generated method stub
    }

    @Override
    public int getPriority() {
        return ExtensionPriority.LOWEST;
    }

}
