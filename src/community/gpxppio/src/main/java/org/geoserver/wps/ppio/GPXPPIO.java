/* Copyright (c) 2012 R3 GIS - www.r3-gis.com. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.ppio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

import org.geoserver.config.ContactInfo;
import org.geoserver.config.GeoServer;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.util.logging.Logging;
import org.hsqldb.lib.StringInputStream;

/**
 * Inputs and outputs feature collections in GPX format using gt-gpx
 * 
 * @author Peter Hopfgartner - R3 GIS
 * 
 */
public class GPXPPIO extends CDataPPIO {
    private static final Logger LOGGER = Logging.getLogger(GPXPPIO.class);
    private GeoServer geoServer;

    protected GPXPPIO(GeoServer geoServer) {
        super(FeatureCollection.class, FeatureCollection.class, "application/gpx+xml");
        this.geoServer = geoServer;
    }

    @Override
    public void encode(Object fc, OutputStream os) throws IOException {

        ContactInfo contact = geoServer.getSettings().getContact();
        GpxEncoder encoder = new GpxEncoder(true);
        encoder.setCreator(contact.getContactOrganization());
        encoder.setLink(contact.getOnlineResource());

        try {
            encoder.encode(os, (SimpleFeatureCollection) fc);
        } catch (Exception e) {
            throw new IOException("Unable to encode in GPX", e);

        }
    }

    @Override
    public Object decode(InputStream input) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("GPX files can not be used as input");
    }

    @Override
    public Object decode(String input) throws UnsupportedOperationException {
        return decode(new StringInputStream(input));
    }

    @Override
    public String getFileExtension() {
        return "gpx";
    }

}
