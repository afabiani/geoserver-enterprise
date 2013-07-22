/* Copyright (c) 2001 - 2013 OpenPlans - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.ppio;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.geoserver.catalog.Catalog;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.resource.WPSResourceManager;
import org.geotools.process.ProcessException;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;

/**
 * Handles input and output of feature collections as zipped files
 * 
 * @author Andrea Aime - OpenGeo
 */
public class ExecutionStatusListPPIO extends BinaryPPIO {

    XStream marshaller = new XStream(new JettisonMappedXmlDriver());

    GeoServer geoServer;

    Catalog catalog;

    WPSResourceManager resources;

    protected ExecutionStatusListPPIO(GeoServer geoServer, WPSResourceManager resources) {
        super(List.class, List.class, "application/json");
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
        this.resources = resources;
    }

    @Override
    public void encode(final Object output, OutputStream os) throws Exception {
        if (output instanceof List) {
            try {
                List<ExecutionStatus> statusList = (List<ExecutionStatus>) output;

                marshaller.toXML(statusList, os);
            } catch (Exception e) {
                throw new ProcessException("Could not encode " + output.getClass());
            }
        }
    }

    @Override
    public Object decode(Object input) throws Exception {
        if (input instanceof String) {
            return marshaller.fromXML((String) input);
        }

        throw new ProcessException("Could not decode " + input.getClass());
    }

    @Override
    public Object decode(InputStream input) throws Exception {
        try {
            Object object = marshaller.fromXML(input);

            if (object instanceof List) {
                try {
                    List<ExecutionStatus> statusList = (List<ExecutionStatus>) object;

                    return statusList;
                } catch (Exception e) {
                    throw new ProcessException("Could not encode " + object.getClass());
                }
            }
        } catch (Exception e) {
            throw new ProcessException("Could not decode " + input.getClass(), e);
        }

        throw new ProcessException("Could not decode " + input.getClass());
    }

    @Override
    public String getFileExtension() {
        return "json";
    }

    public static class ExecutionStatusList extends ExecutionStatusListPPIO {

        public ExecutionStatusList(GeoServer geoServer, WPSResourceManager resources) {
            super(geoServer, resources);
        }

    }
}
