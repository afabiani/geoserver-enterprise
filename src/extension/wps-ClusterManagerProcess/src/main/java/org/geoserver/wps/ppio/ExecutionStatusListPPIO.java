/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
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
 * Handles input and output of feature collections as zipped files.
 *
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class ExecutionStatusListPPIO extends BinaryPPIO {

    /** The marshaller. */
    XStream marshaller = new XStream(new JettisonMappedXmlDriver());

    /** The geo server. */
    GeoServer geoServer;

    /** The catalog. */
    Catalog catalog;

    /** The resources. */
    WPSResourceManager resources;

    /**
     * Instantiates a new execution status list ppio.
     *
     * @param geoServer the geo server
     * @param resources the resources
     */
    protected ExecutionStatusListPPIO(GeoServer geoServer, WPSResourceManager resources) {
        super(List.class, List.class, "application/json");
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
        this.resources = resources;
    }

    /**
     * Encode.
     *
     * @param output the output
     * @param os the os
     * @throws Exception the exception
     */
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

    /**
     * Decode.
     *
     * @param input the input
     * @return the object
     * @throws Exception the exception
     */
    @Override
    public Object decode(Object input) throws Exception {
        if (input instanceof String) {
            return marshaller.fromXML((String) input);
        }

        throw new ProcessException("Could not decode " + input.getClass());
    }

    /**
     * Decode.
     *
     * @param input the input
     * @return the object
     * @throws Exception the exception
     */
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

    /**
     * Gets the file extension.
     *
     * @return the file extension
     */
    @Override
    public String getFileExtension() {
        return "json";
    }

    /**
     * The Class ExecutionStatusList.
     */
    public static class ExecutionStatusList extends ExecutionStatusListPPIO {

        /**
         * Instantiates a new execution status list.
         *
         * @param geoServer the geo server
         * @param resources the resources
         */
        public ExecutionStatusList(GeoServer geoServer, WPSResourceManager resources) {
            super(geoServer, resources);
        }

    }
}
