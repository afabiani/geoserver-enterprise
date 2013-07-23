/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.model.adapter;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

/**
 * The Class GeometryAdapter.
 * 
 * @param <G> the generic type
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class GeometryAdapter<G extends Geometry> extends XmlAdapter<String, G> {

    /*
     * (non-Javadoc)
     * 
     * @see javax.xml.bind.annotation.adapters.XmlAdapter#unmarshal(java.lang.Object)
     */
    /**
     * Unmarshal.
     *
     * @param val the val
     * @return the g
     * @throws ParseException the parse exception
     */
    @SuppressWarnings("unchecked")
    @Override
    public G unmarshal(String val) throws ParseException {
        WKTReader wktReader = new WKTReader();

        Geometry the_geom = wktReader.read(val);
        if (the_geom.getSRID() == 0)
            the_geom.setSRID(4326);

        try {
            return (G) the_geom;
        } catch (ClassCastException e) {
            throw new ParseException("WKT val is a " + the_geom.getClass().getName());
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.xml.bind.annotation.adapters.XmlAdapter#marshal(java.lang.Object)
     */
    /**
     * Marshal.
     *
     * @param the_geom the the_geom
     * @return the string
     * @throws ParseException the parse exception
     */
    @Override
    public String marshal(G the_geom) throws ParseException {
        if (the_geom != null) {
            WKTWriter wktWriter = new WKTWriter();
            if (the_geom.getSRID() == 0)
                the_geom.setSRID(4326);

            return wktWriter.write(the_geom);
        } else {
            throw new ParseException("Geometry obj is null.");
        }
    }
}
