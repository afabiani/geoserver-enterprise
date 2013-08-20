/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.awt.Color;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.image.RenderedImage;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.config.GeoServer;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.process.raster.gs.CropCoverage;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.XAffineTransform;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.lite.gridcoverage2d.GridCoverageRenderer;
import org.geotools.styling.RasterSymbolizerImpl;
import org.geotools.util.logging.Logging;
import org.jaitools.imageutils.ImageLayout2;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.ProgressListener;

import com.sun.media.jai.opimage.RIFUtil;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

/**
 * The Class AbstractDownloadProcess.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public abstract class AbstractDownloadProcess implements GSProcess {

    /** The Constant LOGGER. */
    protected static final Logger LOGGER = Logging.getLogger(AbstractDownloadProcess.class);

    /** The geo server. */
    protected GeoServer geoServer;

    /** The catalog. */
    protected Catalog catalog;

    /** The geom builder. */
    protected GeometryBuilder geomBuilder = new GeometryBuilder();

    /**
     * Instantiates a new abstract download process.
     * 
     * @param geoServer the geo server
     */
    public AbstractDownloadProcess(GeoServer geoServer) {
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
    }

    /**
     * Execute.
     * 
     * @param layerName the layer name
     * @param filter the filter
     * @param email the email
     * @param outputFormat the output format
     * @param targetCRS the target crs
     * @param roiCRS the roi crs
     * @param roi the roi
     * @param cropToGeometry the crop to geometry
     * @param progressListener the progress listener
     * @return the object
     * @throws ProcessException the process exception
     */
    @DescribeResult(name = "result", description = "Zipped output files to download")
    public abstract Object execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vectorial Filter") Filter filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 0, description = "Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "RoiCRS", min = 0, description = "Region Of Interest CRS") CoordinateReferenceSystem roiCRS,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            final ProgressListener progressListener) throws ProcessException;

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
    protected GridCoverage2D getCoverage(ResourceInfo resourceInfo, CoverageInfo coverage,
            Geometry roi, CoordinateReferenceSystem roiCRS, CoordinateReferenceSystem targetCRS,
            boolean cropToGeometry, ProgressListener progressListener) throws Exception {
        Throwable cause = null;

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
                    "Grid to world tranform is not an AffineTransform:" + resourceInfo.getName());
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
    protected GridCoverage2D getFinalCoverage(ResourceInfo resourceInfo, CoverageInfo coverage,
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
        } catch (MismatchedDimensionException e) {
            cause = e;
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        } catch (TransformException e) {
            cause = e;
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        } catch (FactoryException e) {
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

            } catch (TransformException e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
            } catch (FactoryException e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
            } catch (NumberFormatException e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
            } catch (NoninvertibleTransformException e) {
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
            } catch (TransformException e) {
                cause = e;
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);

            } catch (FactoryException e) {
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

    /**
     * Chek target crs validity.
     * 
     * @param targetCRS the target crs
     * @param referenceCRS the reference crs
     * @param needResample the need resample
     * @param progressListener the progress listener
     * @return true, if successful
     */
    private boolean checkTargetCRSValidity(CoordinateReferenceSystem targetCRS,
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