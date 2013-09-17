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

import java.awt.image.SampleModel;
import java.text.DecimalFormat;
import java.util.List;
import java.util.logging.Logger;

import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.LiteralPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.data.Parameter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.process.ProcessException;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
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
    final static GridCoverageFactory GC_FACTORY = new GridCoverageFactory();

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logging.getLogger(DownloadUtilities.class);

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
     * @param roi
     * @throws IllegalStateException
     */
    static void checkPolygonROI(Geometry roi) throws IllegalStateException {
        if (roi instanceof Point || roi instanceof MultiPoint || roi instanceof LineString
                || roi instanceof MultiLineString) {
            throw new IllegalStateException(
                    "The Region of Interest is not a Polygon or Multipolygon!");
        }
        if (roi.isEmpty() || !roi.isValid()) {
            throw new IllegalStateException("The Region of Interest is empyt or invalid!");
        }
    }

    final static ProcessParameterIO find(Parameter<?> p, ApplicationContext context, String mime,
            boolean lenient) {
        //
        // lenient approach, try to give something back in any case
        //
        if (lenient) {
            return ProcessParameterIO.find(p, context, mime);
        }

        //
        // Strict match case. If we don't find a match we return null
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

        // unable to find a match
        return null;
    }

    /**
     * @param clippedFeatures
     * @throws IllegalStateException
     */
    final static void checkIsEmptyFeatureCollection(SimpleFeatureCollection clippedFeatures)
            throws IllegalStateException {
        if (clippedFeatures == null || clippedFeatures.isEmpty()) {
            throw new IllegalStateException("Got an empty feature collection.");
        }
    }

    /**
     * Chek target crs validity.
     * 
     * @param targetCRS the target crs
     * @param referenceCRS the reference crs
     * @param needResample the need resample
     * @param progressListener the progress listener
     * @return true, if successful
     */
    static boolean checkTargetCRSValidity(CoordinateReferenceSystem targetCRS,
            CoordinateReferenceSystem referenceCRS, boolean needResample,
            ProgressListener progressListener) {
        if (targetCRS != null) {
            if (!CRS.equalsIgnoreMetadata(referenceCRS, targetCRS)) {

                // testing reprojection...
                try {
                    /* if (! ( */CRS.findMathTransform(referenceCRS, targetCRS) /*
                                                                                 * instanceof AffineTransform) ) throw new ProcessException
                                                                                 * ("Could not reproject to reference CRS")
                                                                                 */;
                } catch (Exception e) {
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Could not reproject to reference CRS", e));
                    }
                    throw new ProcessException("Could not reproject to reference CRS", e);
                }

                needResample = true;
            }
        }
        return needResample;
    }

}
