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

import java.awt.Color;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.image.ColorModel;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;

import org.geoserver.catalog.CoverageInfo;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.LiteralPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.Parameter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.raster.gs.CropCoverage;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.XAffineTransform;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.lite.gridcoverage2d.GridCoverageRenderer;
import org.geotools.styling.RasterSymbolizerImpl;
import org.geotools.util.logging.Logging;
import org.jaitools.imageutils.ImageLayout2;
import org.opengis.coverage.ColorInterpretation;
import org.opengis.geometry.Envelope;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;
import org.springframework.context.ApplicationContext;

import com.sun.media.jai.opimage.RIFUtil;
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
    
        return GC_FACTORY.create(name, raster, new GeneralEnvelope(envelope), bands, null, null);
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
        if (roi instanceof Point || roi instanceof MultiPoint ||
                roi instanceof LineString || roi instanceof MultiLineString) {
                throw new IllegalStateException("The Region of Interest is not a Polygon or Multipolygon!");
        }
        if(roi.isEmpty()||!roi.isValid()){
            throw new IllegalStateException("The Region of Interest is empyt or invalid!");
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

    /**
     * Gets the coverage.
     * 
     * @param resourceInfo the resource info
     * @param coverage the coverage
     * @param roi the roi
     * @param roiCRS the roi crs
     * @param targetCRS the target crs
     * @param cropToGeometry the crop to geometry
     * @param progressListener the progress listener
     * @return the coverage
     * @throws Exception the exception
     */
    static GridCoverage2D getCoverage(
            CoverageInfo coverage,
            Geometry roi, 
            CoordinateReferenceSystem roiCRS, 
            CoordinateReferenceSystem targetCRS,
            boolean cropToGeometry, 
            ProgressListener progressListener) throws Exception {
        Throwable cause = null;
    
        CoordinateReferenceSystem referenceCRS = coverage.getCRS();
        ReferencedEnvelope finalEnvelope = null;
        try {
            finalEnvelope = coverage.getNativeBoundingBox();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
    
        // prepare the envelope and make sure the CRS is set
    
        // use ROI if present
        boolean needResample = false;
        if (roi != null) {
            // reproject the coverage envelope if needed
            needResample = checkTargetCRSValidity(targetCRS, referenceCRS, needResample,
                    progressListener);
    
            com.vividsolutions.jts.geom.Envelope envelope = null;
            ReferencedEnvelope refEnvelope = null;
            if (needResample) {
                roi = JTS.transform(roi, CRS.findMathTransform(roiCRS, targetCRS));
            } else {
                roi = JTS.transform(roi, CRS.findMathTransform(roiCRS, referenceCRS));
            }
    
            envelope = roi.getEnvelopeInternal();
            refEnvelope = new ReferencedEnvelope(envelope, (targetCRS != null ? targetCRS
                    : referenceCRS));
            refEnvelope = refEnvelope.transform(referenceCRS, true);
            finalEnvelope = new ReferencedEnvelope(refEnvelope.intersection(finalEnvelope),
                    referenceCRS);
        } else {
            // reproject the coverage envelope if needed
            needResample = checkTargetCRSValidity(targetCRS, referenceCRS, needResample,
                    progressListener);
        }
    
        final GeneralEnvelope envelope = new GeneralEnvelope(finalEnvelope);
        envelope.setCoordinateReferenceSystem(referenceCRS);
    
        // ---- START - Envelope and geometry sanity checks
        // check envelope and ROI to make sure it is not empty
        if (envelope.isEmpty()) {
            progressListener.progress(100);
            progressListener.setCanceled(true);
            cause = new IllegalStateException("Final envelope is empty!");
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        }
    
        if ((envelope.getLowerCorner().getOrdinate(0) == envelope.getUpperCorner().getOrdinate(0))
                || (envelope.getLowerCorner().getOrdinate(1) == envelope.getUpperCorner()
                        .getOrdinate(1))) {
            if (progressListener != null) {
                progressListener
                        .exceptionOccurred(new ProcessException(
                                "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!"));
            }
            throw new ProcessException(
                    "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!");
        }
        // ---- END - Envelope and geometry sanity checks
    
        // pixel scale
        final MathTransform tempTransform = coverage.getGrid().getGridToCRS();
        if (!(tempTransform instanceof AffineTransform)) {
            cause = new IllegalArgumentException(
                    "Grid to world tranform is not an AffineTransform:" + coverage.getName());
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        }
        AffineTransform tr = (AffineTransform) tempTransform;
    
        // resolution
        double pixelSizesX = XAffineTransform.getScaleX0(tr);
        double pixelSizesY = XAffineTransform.getScaleY0(tr);
    
        // G2W transform
        final AffineTransform2D g2w = new AffineTransform2D(pixelSizesX, 0, 0, -pixelSizesY,
                envelope.getLowerCorner().getOrdinate(0), envelope.getUpperCorner().getOrdinate(1));
    
        // hints for tiling
        final Hints hints = GeoTools.getDefaultHints().clone();
        final ImageLayout2 layout = new ImageLayout2();
        layout.setTileWidth(JAI.getDefaultTileSize().width);
        layout.setTileHeight(JAI.getDefaultTileSize().height);
        hints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, layout));
    
        // prepare final gridgeometry
        GridGeometry2D finalGridGeometry = new GridGeometry2D(PixelInCell.CELL_CORNER, g2w,
                envelope, hints);
    
        // prepare final coverage
        // === make sure we read in streaming and we read just what we need
        final ParameterValue<Boolean> streamingRead = AbstractGridFormat.USE_JAI_IMAGEREAD
                .createValue();
        streamingRead.setValue(true);
    
        final ParameterValue<GridGeometry2D> readGG = AbstractGridFormat.READ_GRIDGEOMETRY2D
                .createValue();
        readGG.setValue(finalGridGeometry);
    
        final ParameterValue<String> suggestedTileSize = AbstractGridFormat.SUGGESTED_TILE_SIZE
                .createValue();
        final ImageLayout readLayout = RIFUtil.getImageLayoutHint(hints);
        if (readLayout != null && readLayout.isValid(ImageLayout.TILE_HEIGHT_MASK)
                && readLayout.isValid(ImageLayout.TILE_WIDTH_MASK)) {
            suggestedTileSize.setValue(String.valueOf(readLayout.getTileWidth(null)) + ","
                    + String.valueOf(readLayout.getTileHeight(null)));
        } else {
            // default
            suggestedTileSize.setValue(String.valueOf(JAI.getDefaultTileSize().width) + ","
                    + String.valueOf(JAI.getDefaultTileSize().height));
        }
        GridCoverage2D gc = (GridCoverage2D) coverage.getGridCoverageReader(
                progressListener, hints).read(
                new GeneralParameterValue[] { streamingRead, readGG, suggestedTileSize });
    
        return gc;
    }

    /**
     * Gets the final coverage.
     * 
     * @param resourceInfo the resource info
     * @param coverage the coverage
     * @param gc the gc
     * @param roi the roi
     * @param roiCRS the roi crs
     * @param targetCRS the target crs
     * @param cropToGeometry the crop to geometry
     * @param progressListener the progress listener
     * @return the final coverage
     */
    static GridCoverage2D getFinalCoverage(CoverageInfo coverage,
            GridCoverage2D gc, Geometry roi, CoordinateReferenceSystem roiCRS,
            CoordinateReferenceSystem targetCRS, boolean cropToGeometry,
            ProgressListener progressListener) {
        Throwable cause = null;
        GridCoverage2D finalCoverage = null;
        // create return coverage reusing origin grid to world
        RenderedImage raster = null;
    
        CoordinateReferenceSystem referenceCRS = coverage.getCRS();
        ReferencedEnvelope finalEnvelope = null;
        try {
            finalEnvelope = coverage.getNativeBoundingBox();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
    
        // simulate reprojection
        // tr=new GridToEnvelopeMapper(coverage.getGrid().getGridRange(), finalEnvelope).createAffineTransform();
    
        // prepare the envelope and make sure the CRS is set
    
        // use ROI if present
        boolean needResample = false;
        try {
            if (roi != null) {
                // reproject the coverage envelope if needed
                needResample = DownloadUtilities.checkTargetCRSValidity(targetCRS, referenceCRS, needResample,
                        progressListener);
    
                com.vividsolutions.jts.geom.Envelope envelope = null;
                ReferencedEnvelope refEnvelope = null;
                if (needResample) {
                    roi = JTS.transform(roi, CRS.findMathTransform(roiCRS, targetCRS));
                } else {
                    roi = JTS.transform(roi, CRS.findMathTransform(roiCRS, referenceCRS));
                }
    
                envelope = roi.getEnvelopeInternal();
                refEnvelope = new ReferencedEnvelope(envelope, (targetCRS != null ? targetCRS
                        : referenceCRS));
                refEnvelope = refEnvelope.transform(referenceCRS, true);
                finalEnvelope = new ReferencedEnvelope(refEnvelope.intersection(finalEnvelope),
                        referenceCRS);
            } else {
                // reproject the coverage envelope if needed
                needResample = DownloadUtilities.checkTargetCRSValidity(targetCRS, referenceCRS, needResample,
                        progressListener);
            }
        } catch (Exception e) {
            cause = e;
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        } 
    
        if (needResample) {
            // hints for tiling
            final Hints hints = GeoTools.getDefaultHints().clone();
            final ImageLayout2 layout = new ImageLayout2();
            layout.setTileWidth(JAI.getDefaultTileSize().width);
            layout.setTileHeight(JAI.getDefaultTileSize().height);
            hints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, layout));
    
            hints.add(new RenderingHints(JAI.KEY_INTERPOLATION, Interpolation
                    .getInstance(Interpolation.INTERP_NEAREST)));
            ReferencedEnvelope targetEnvelope;
            try {
                targetEnvelope = finalEnvelope.transform(targetCRS, true);
                GridCoverageRenderer renderer = new GridCoverageRenderer(targetCRS, targetEnvelope,
                        gc.getGridGeometry().getGridRange2D().getBounds(), (AffineTransform) null,
                        hints);
                final ParameterValue<String> suggestedTileSize = AbstractGridFormat.SUGGESTED_TILE_SIZE
                        .createValue();
                raster = renderer.renderImage(gc, new RasterSymbolizerImpl(),
                        Interpolation.getInstance(Interpolation.INTERP_NEAREST), Color.WHITE,
                        Integer.parseInt(suggestedTileSize.getValue().split(",")[1]),
                        Integer.parseInt(suggestedTileSize.getValue().split(",")[0]));
    
                finalCoverage = DownloadUtilities.createCoverage("resampled", raster, targetEnvelope);
    
            } catch (Exception e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
            } 
        } else {
            finalCoverage = gc;
        }
    
        // ---- Cropping coverage to the Region of Interest
        if (roi != null && cropToGeometry) {
    
            Geometry cropShape = roi;
            ReferencedEnvelope finalEnvelopeInTargetCRS;
            try {
                finalEnvelopeInTargetCRS = (needResample ? finalEnvelope.transform(targetCRS, true)
                        : finalEnvelope);
            } catch (Exception e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
            }
    
            double x1 = finalEnvelopeInTargetCRS.getLowerCorner().getOrdinate(0);
            double y1 = finalEnvelopeInTargetCRS.getLowerCorner().getOrdinate(1);
            double x2 = finalEnvelopeInTargetCRS.getUpperCorner().getOrdinate(0);
            double y2 = finalEnvelopeInTargetCRS.getUpperCorner().getOrdinate(1);
            com.vividsolutions.jts.geom.Envelope coverageEnvelope = new com.vividsolutions.jts.geom.Envelope(
                    x1, x2, y1, y2);
            cropShape = cropShape.intersection(JTS.toGeometry(coverageEnvelope));
    
            if ((x1 == x2) || (y1 == y2)) {
                throw new ProcessException(
                        "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!");
            }
            if (cropShape instanceof Point || cropShape instanceof MultiPoint) {
                throw new ProcessException("The Region of Interest is not a valid geometry!");
            }
    
            CropCoverage cropProcess = new CropCoverage();
            try {
                finalCoverage = cropProcess.execute(finalCoverage, cropShape, progressListener);
            } catch (IOException e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
            }
        }
    
        return finalCoverage;
    }

}
