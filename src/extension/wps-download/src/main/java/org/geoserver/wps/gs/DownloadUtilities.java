/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2011, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geoserver.wps.gs;

import java.awt.image.ColorModel;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletContext;

import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wfs.response.ShapeZipOutputFormat;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.LiteralPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.data.DataStore;
import org.geotools.data.Parameter;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.process.ProcessException;
import org.opengis.coverage.ColorInterpretation;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.Envelope;
import org.opengis.util.ProgressListener;
import org.springframework.context.ApplicationContext;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

/**
 * Various Utilities for Download Services.
 * 
 * @author Simone Giannecchini, GeoSolutions SAS
 *
 */
final class DownloadUtilities {

    /** The gc factory. */
    final static GridCoverageFactory gcFactory = new GridCoverageFactory();

    /**
     * Singleton
     */
    private DownloadUtilities() {
    }

    /**
     * Computes the number of pixels for this {@link GridEnvelope2D}.
     * 
     * @param rasterEnvelope the {@link GridEnvelope2D} to compute the number of pixels for
     * @return the number of pixels for the provided {@link GridEnvelope2D}
     */
    static long computePixelsNumber(GridEnvelope2D rasterEnvelope) {
        // pixels
        long pixelsNumber = 1;
        final int dimensions = rasterEnvelope.getDimension();
        for (int i = 0; i < dimensions; i++) {
            pixelsNumber *= rasterEnvelope.getSpan(i);
        }
        return pixelsNumber;
    }

    /**
     * Creates the coverage.
     * 
     * @param name the name
     * @param raster the raster
     * @param envelope the envelope
     * @return the grid coverage2 d
     */
    static GridCoverage2D createCoverage(final String name, final RenderedImage raster,
            final Envelope envelope) {
    
        // creating bands
        final SampleModel sm = raster.getSampleModel();
        final ColorModel cm = raster.getColorModel();
        final int numBands = sm.getNumBands();
        final GridSampleDimension[] bands = new GridSampleDimension[numBands];
        // setting bands names.
        for (int i = 0; i < numBands; i++) {
            final ColorInterpretation colorInterpretation = TypeMap.getColorInterpretation(cm, i);
            final String sdName = (colorInterpretation == null) ? ("band" + i)
                    : colorInterpretation.name();
            bands[i] = new GridSampleDimension(sdName).geophysics(true);
        }
    
        return gcFactory.create(name, raster, new GeneralEnvelope(envelope), bands, null, null);
    }

    /**
     * Utility function to format a byte amount into a human readable string.
     * 
     * @param bytes the bytes
     * @return the string
     */
    static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        } else if (bytes < 1024 * 1024) {
            return new DecimalFormat("#.##").format(bytes / 1024.0) + "KB";
        } else {
            return new DecimalFormat("#.##").format(bytes / 1024.0 / 1024.0) + "MB";
        }
    }

    /**
     * Gets the charset.
     * 
     * @return the charset
     */
    static Charset getCharset() {
        final String charsetName = GeoServerExtensions.getProperty(
                ShapeZipOutputFormat.GS_SHAPEFILE_CHARSET, (ServletContext) null);
        if (charsetName != null) {
            return Charset.forName(charsetName);
        } else {
            // if not specified let's use the shapefile default one
            return Charset.forName("ISO-8859-1");
        }
    }

    /**
     * Computes the size of a grid coverage given its grid envelope and the target sample model.
     * 
     * @param envelope the envelope
     * @param sm the sm
     * @return the coverage size
     */
    static long getCoverageSize(GridEnvelope2D envelope, SampleModel sm) {
        // === compute the coverage memory usage and compare with limit
        final long pixelsNumber = computePixelsNumber(envelope);
    
        long pixelSize = 0;
        final int numBands = sm.getNumBands();
        for (int i = 0; i < numBands; i++) {
            pixelSize += sm.getSampleSize(i);
        }
        return pixelsNumber * pixelSize / 8;
    }

    /**
     * Gets the feature source.
     * 
     * @param dataStore the data store
     * @param resourceInfo the resource info
     * @param progressListener the progress listener
     * @return the feature source
     * @throws Exception the exception
     */
    static SimpleFeatureSource getFeatureSource(DataStoreInfo dataStore,
            ResourceInfo resourceInfo, ProgressListener progressListener) throws Exception {
        SimpleFeatureType targetType;
        // grab the data store
        DataStore ds = (DataStore) dataStore.getDataStore(progressListener);
    
        // try to get the target feature type (might have slightly different name and structure)
        String type = "";
        for (String typeName : ds.getTypeNames()) {
            if (typeName.equalsIgnoreCase(resourceInfo.getName())) {
                type = typeName;
            }
        }
    
        targetType = ds.getSchema(type);
        if (targetType == null) {
            // ouch, the name was changed... we can only guess now...
            // try with the typical Oracle mangling
            targetType = ds.getSchema(type.toUpperCase());
        }
    
        if (targetType == null) {
            Throwable cause = new WPSException(
                    "No TypeName detected on source schema.Cannot proceed further.");
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        }
    
        // get the features
        return ds.getFeatureSource(targetType.getTypeName());
    }

    /**
     * @param roiInNativeCRS
     * @throws IllegalStateException
     */
    static void checkPolygonROI(Geometry roiInNativeCRS) throws IllegalStateException {
        if (roiInNativeCRS instanceof Point || roiInNativeCRS instanceof MultiPoint ||
                roiInNativeCRS instanceof LineString || roiInNativeCRS instanceof MultiLineString) {
                throw new IllegalStateException("The Region of Interest is not a valid geometry after going back to native CRS!");
        }
    }
    
    final static ProcessParameterIO find(Parameter<?> p, ApplicationContext context, String mime, boolean lenient) {
        // 
        // lenient approach, try to give something back in any case
        //
        if(lenient){
            return ProcessParameterIO.find(p, context, mime);
        }
        
        //
        // Strict match case. If we don't find a match we  return null
        //
        // enum special treatment
        if (p.type.isEnum()) {
            return new LiteralPPIO(p.type);
        }

        // TODO: come up with some way to flag one as "default"
        List<ProcessParameterIO> all = ProcessParameterIO.findAll(p, context);
        if (all.isEmpty()) {
            return null;
        }

        if (mime != null) {
            for (ProcessParameterIO ppio : all) {
                if (ppio instanceof ComplexPPIO && ((ComplexPPIO) ppio).getMimeType().equals(mime)) {
                    return ppio;
                }
            }
        }

        //unable to find a match
        return null;
    }

}
