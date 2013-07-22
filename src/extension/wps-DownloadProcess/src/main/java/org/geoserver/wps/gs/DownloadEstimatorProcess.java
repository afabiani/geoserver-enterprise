/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.awt.Color;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.image.ColorModel;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.WPSException;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.InvalidGridGeometryException;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.process.raster.gs.CropCoverage;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.matrix.XAffineTransform;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.lite.gridcoverage2d.GridCoverageRenderer;
import org.geotools.styling.RasterSymbolizerImpl;
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;
import org.jaitools.imageutils.ImageLayout2;
import org.opengis.coverage.ColorInterpretation;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.geometry.Envelope;
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

@DescribeProcess(title = "Enterprise Download Process", description = "Downloads Layer Stream and provides a ZIP.")
public class DownloadEstimatorProcess implements GSProcess {

    protected static final Logger LOGGER = Logging.getLogger(DownloadEstimatorProcess.class);

    private static final long DEFAULT_MAX_FEATURES = 1000000;

    private static GridCoverageFactory gcFactory = new GridCoverageFactory();

    protected GeoServer geoServer;

    protected Catalog catalog;

    protected FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    protected GeometryBuilder geomBuilder = new GeometryBuilder();

    private long maxFeatures;

    private long readLimits;

    private long writeLimits;

    public DownloadEstimatorProcess(GeoServer geoServer) {
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
        this.maxFeatures = DEFAULT_MAX_FEATURES;
        this.readLimits = 0;
        this.writeLimits = 0;
    }

    @DescribeResult(name = "result", description = "Download Limits are respected or not!")
    public Boolean execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vectorial Filter") String filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 1, description = "Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "ROI", min = 1, description = "Region Of Interest") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            ProgressListener progressListener) throws ProcessException {

        Throwable cause = null;

        if (layerName != null) {
            final LayerInfo layerInfo = catalog.getLayerByName(layerName);
            final ResourceInfo resourceInfo = layerInfo.getResource();
            final StoreInfo storeInfo = resourceInfo.getStore();

            if (storeInfo == null) {
                cause = new IllegalArgumentException("Unable to locate feature:"
                        + resourceInfo.getName());
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(
                            "Could not complete the Download Process", cause));
                }
                throw new ProcessException("Could not complete the Download Process", cause);
            }

            if (storeInfo instanceof DataStoreInfo) {
                final DataStoreInfo dataStore = (DataStoreInfo) storeInfo;

                try {
                    // === filter or expression
                    Filter ra = null;
                    if (filter != null) {
                        try {
                            ra = ECQL.toFilter(filter);
                        } catch (Exception e) {
                            cause = new WPSException("Unable to parse input expression", e);
                            if (progressListener != null) {
                                progressListener.exceptionOccurred(new ProcessException(
                                        "Could not complete the Download Process", cause));
                            }
                            throw new ProcessException("Could not complete the Download Process",
                                    cause);
                        }
                    } else {
                        ra = Filter.INCLUDE;
                    }

                    SimpleFeatureType targetType;
                    // grab the data store
                    DataStore ds = (DataStore) dataStore.getDataStore(null);

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
                        cause = new WPSException(
                                "No TypeName detected on source schema.Cannot proceeed further.");
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    }

                    // get the features
                    final SimpleFeatureSource featureSource = ds.getFeatureSource(targetType
                            .getTypeName());

                    SimpleFeatureCollection features = featureSource.getFeatures(ra);

                    if (features.size() > maxFeatures) {
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Features Download Limits: Max allowed features exceeded."));
                        }
                        throw new ProcessException("Max allowed of " + this.maxFeatures
                                + " features exceeded.");
                    }
                } catch (Exception e) {
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Error while checking Feature Download Limits", e));
                    }
                    throw new ProcessException("Error while checking Feature Download Limits", e);
                }

                return (true);
            } else if (storeInfo instanceof CoverageStoreInfo) {
                final CoverageStoreInfo coverageStore = (CoverageStoreInfo) storeInfo;
                final CoverageInfo coverage = catalog.getCoverageByName(resourceInfo.getName());

                if (coverageStore == null || coverage == null) {
                    cause = new IllegalArgumentException("Unable to locate coverage:"
                            + resourceInfo.getName());
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Could not complete the Download Process", cause));
                    }
                    throw new ProcessException("Could not complete the Download Process", cause);
                } else {
                    try {
                        /**
                         * Checking that the coverage described by the specified geometry and sample model does not exceeds the read limits
                         */
                        // pixel scale
                        final MathTransform tempTransform = coverage.getGrid().getGridToCRS();
                        if (!(tempTransform instanceof AffineTransform)) {
                            cause = new IllegalArgumentException(
                                    "Grid to world tranform is not an AffineTransform:"
                                            + resourceInfo.getName());
                            if (progressListener != null) {
                                progressListener.exceptionOccurred(new ProcessException(
                                        "Could not complete the Download Process", cause));
                            }
                            throw new ProcessException("Could not complete the Download Process",
                                    cause);
                        }
                        AffineTransform tr = (AffineTransform) tempTransform;

                        CoordinateReferenceSystem referenceCRS = coverage.getCRS();
                        ReferencedEnvelope finalEnvelope = null;
                        try {
                            finalEnvelope = coverage.getNativeBoundingBox();
                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING, e.getMessage(), e);
                        }

                        // simulate reprojection
                        // tr=new GridToEnvelopeMapper(coverage.getGrid().getGridRange(), finalEnvelope).createAffineTransform();

                        // resolution
                        double pixelSizesX = XAffineTransform.getScaleX0(tr);
                        double pixelSizesY = XAffineTransform.getScaleY0(tr);

                        // prepare the envelope and make sure the CRS is set

                        // use ROI if present
                        boolean needResample = false;
                        if (roi != null) {
                            final com.vividsolutions.jts.geom.Envelope envelope = roi
                                    .getEnvelopeInternal();

                            ReferencedEnvelope refEnvelope = new ReferencedEnvelope(envelope,
                                    targetCRS);

                            // reproject the coverage envelope if needed
                            if (!CRS.equalsIgnoreMetadata(targetCRS, referenceCRS)) {

                                // testing reprojection...
                                try {
                                    /* if (! ( */CRS.findMathTransform(targetCRS, referenceCRS) /*
                                                                                                 * instanceof AffineTransform) ) throw new
                                                                                                 * ProcessException
                                                                                                 * ("Could not reproject to reference CRS")
                                                                                                 */;
                                } catch (Exception e) {
                                    if (progressListener != null) {
                                        progressListener.exceptionOccurred(new ProcessException(
                                                "Could not reproject to reference CRS", e));
                                    }
                                    throw new ProcessException(
                                            "Could not reproject to reference CRS", e);
                                }

                                refEnvelope = refEnvelope.transform(referenceCRS, true);
                                needResample = true;
                            }

                            finalEnvelope = new ReferencedEnvelope(
                                    refEnvelope.intersection(finalEnvelope), referenceCRS);
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
                                progressListener.exceptionOccurred(new ProcessException(
                                        "Could not complete the Download Process", cause));
                            }
                            throw new ProcessException("Could not complete the Download Process",
                                    cause);
                        }

                        if ((envelope.getLowerCorner().getOrdinate(0) == envelope.getUpperCorner()
                                .getOrdinate(0))
                                || (envelope.getLowerCorner().getOrdinate(1) == envelope
                                        .getUpperCorner().getOrdinate(1))) {
                            if (progressListener != null) {
                                progressListener
                                        .exceptionOccurred(new ProcessException(
                                                "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!"));
                            }
                            throw new ProcessException(
                                    "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!");
                        }

                        Geometry cropShape = roi;
                        ReferencedEnvelope finalEnvelopeInTargetCRS = finalEnvelope.transform(
                                targetCRS, true);
                        double x1 = finalEnvelopeInTargetCRS.getLowerCorner().getOrdinate(0);
                        double y1 = finalEnvelopeInTargetCRS.getLowerCorner().getOrdinate(1);
                        double x2 = finalEnvelopeInTargetCRS.getUpperCorner().getOrdinate(0);
                        double y2 = finalEnvelopeInTargetCRS.getUpperCorner().getOrdinate(1);
                        com.vividsolutions.jts.geom.Envelope coverageEnvelope = new com.vividsolutions.jts.geom.Envelope(
                                x1, x2, y1, y2);
                        cropShape = cropShape.intersection(JTS.toGeometry(coverageEnvelope));

                        if ((x1 == x2) || (y1 == y2)) {
                            if (progressListener != null) {
                                progressListener
                                        .exceptionOccurred(new ProcessException(
                                                "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!"));
                            }
                            throw new ProcessException(
                                    "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!");
                        }
                        if (cropShape instanceof Point || cropShape instanceof MultiPoint) {
                            if (progressListener != null) {
                                progressListener.exceptionOccurred(new ProcessException(
                                        "The Region of Interest is not a valid geometry!"));
                            }
                            throw new ProcessException(
                                    "The Region of Interest is not a valid geometry!");
                        }
                        // ---- END - Envelope and geometry sanity checks

                        // G2W transform
                        final AffineTransform2D g2w = new AffineTransform2D(pixelSizesX, 0, 0,
                                -pixelSizesY, envelope.getLowerCorner().getOrdinate(0), envelope
                                        .getUpperCorner().getOrdinate(1));

                        // hints for tiling
                        final Hints hints = GeoTools.getDefaultHints().clone();
                        final ImageLayout2 layout = new ImageLayout2();
                        layout.setTileWidth(JAI.getDefaultTileSize().width);
                        layout.setTileHeight(JAI.getDefaultTileSize().height);
                        hints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, layout));

                        // prepare final gridgeometry
                        GridGeometry2D finalGridGeometry = new GridGeometry2D(
                                PixelInCell.CELL_CORNER, g2w, envelope, hints);

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
                            suggestedTileSize
                                    .setValue(String.valueOf(readLayout.getTileWidth(null)) + ","
                                            + String.valueOf(readLayout.getTileHeight(null)));
                        } else {
                            // default
                            suggestedTileSize
                                    .setValue(String.valueOf(JAI.getDefaultTileSize().width) + ","
                                            + String.valueOf(JAI.getDefaultTileSize().height));
                        }
                        final GridCoverage2D gc = (GridCoverage2D) coverage.getGridCoverageReader(
                                new NullProgressListener(), hints).read(
                                new GeneralParameterValue[] { streamingRead, readGG,
                                        suggestedTileSize });

                        // compute the coverage memory usage and compare with limit
                        long actual = getCoverageSize(gc.getGridGeometry().getGridRange2D(), gc
                                .getRenderedImage().getSampleModel());
                        if (readLimits > 0 && actual > readLimits * 1024) {
                            if (progressListener != null) {
                                progressListener.exceptionOccurred(new ProcessException(
                                        "This request is trying to read too much data, "
                                                + "the limit is " + formatBytes(readLimits)
                                                + " but the actual amount of "
                                                + "bytes to be read is " + formatBytes(actual)));
                            }
                            throw new ProcessException(
                                    "This request is trying to read too much data, "
                                            + "the limit is " + formatBytes(readLimits)
                                            + " but the actual amount of " + "bytes to be read is "
                                            + formatBytes(actual));
                        }

                        /**
                         * Checking that the coverage described by the specified geometry and sample model does not exceeds the output limits
                         */
                        // create return coverage reusing origin grid to world
                        GridCoverage2D finalCovergae = null;
                        RenderedImage raster = null;
                        if (needResample) {
                            hints.add(new RenderingHints(JAI.KEY_INTERPOLATION, Interpolation
                                    .getInstance(Interpolation.INTERP_NEAREST)));
                            ReferencedEnvelope targetEnvelope = finalEnvelope.transform(targetCRS,
                                    true);
                            GridCoverageRenderer renderer = new GridCoverageRenderer(targetCRS,
                                    targetEnvelope, gc.getGridGeometry().getGridRange2D()
                                            .getBounds(), (AffineTransform) null, hints);
                            raster = renderer.renderImage(gc, new RasterSymbolizerImpl(),
                                    Interpolation.getInstance(Interpolation.INTERP_NEAREST),
                                    Color.WHITE,
                                    Integer.parseInt(suggestedTileSize.getValue().split(",")[1]),
                                    Integer.parseInt(suggestedTileSize.getValue().split(",")[0]));

                            finalCovergae = createCoverage("resampled", raster, targetEnvelope);
                        } else {
                            finalCovergae = gc;
                        }

                        // ---- Cropping coverage to the Region of Interest
                        CropCoverage cropProcess = new CropCoverage();
                        finalCovergae = cropProcess.execute(finalCovergae, cropShape,
                                progressListener);

                        // compute the coverage memory usage and compare with limit
                        actual = getCoverageSize(finalCovergae.getGridGeometry().getGridRange2D(),
                                finalCovergae.getRenderedImage().getSampleModel());
                        if (writeLimits > 0 && actual > writeLimits * 1024) {
                            if (progressListener != null) {
                                progressListener
                                        .exceptionOccurred(new ProcessException(
                                                "This request is trying to generate too much data, "
                                                        + "the limit is "
                                                        + formatBytes(writeLimits)
                                                        + " but the actual amount of bytes to be "
                                                        + "written in the output is "
                                                        + formatBytes(actual)));
                            }
                            throw new ProcessException(
                                    "This request is trying to generate too much data, "
                                            + "the limit is " + formatBytes(writeLimits)
                                            + " but the actual amount of bytes to be "
                                            + "written in the output is " + formatBytes(actual));
                        }

                    } catch (IOException e) {
                        cause = e;
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    } catch (TransformException e) {
                        cause = e;
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    } catch (FactoryException e) {
                        cause = e;
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    } catch (InvalidGridGeometryException e) {
                        cause = e;
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    } catch (NoninvertibleTransformException e) {
                        cause = e;
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    }

                    return (true);
                }
            } else {
                cause = new WPSException("Could not find store for layer " + layerName);
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new WPSException(
                            "Could not find store for layer " + layerName));
                }
            }
        } else {
            cause = new WPSException("Could not find layer " + layerName);
            if (progressListener != null) {
                progressListener.exceptionOccurred(new WPSException("Could not find layer "
                        + layerName));
            }
        }

        if (progressListener != null) {
            progressListener.exceptionOccurred(new ProcessException(
                    "Could not complete the Download Process", cause));
        }
        throw new ProcessException("Could not complete the Download Process", cause);
    }

    /**
     * @param maxFeatures the maxFeatures to set
     */
    public void setMaxFeatures(long maxFeatures) {
        this.maxFeatures = maxFeatures;
    }

    /**
     * @return the maxFeatures
     */
    public long getMaxFeatures() {
        return maxFeatures;
    }

    /**
     * @param readLimits the readLimits to set
     */
    public void setReadLimits(long readLimits) {
        this.readLimits = readLimits;
    }

    /**
     * @return the readLimits
     */
    public long getReadLimits() {
        return readLimits;
    }

    /**
     * @param writeLimits the writeLimits to set
     */
    public void setWriteLimits(long writeLimits) {
        this.writeLimits = writeLimits;
    }

    /**
     * @return the writeLimits
     */
    public long getWriteLimits() {
        return writeLimits;
    }

    /**
     * 
     * @param name
     * @param raster
     * @param envelope
     * @return
     */
    public static GridCoverage2D createCoverage(final String name, final RenderedImage raster,
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
     * Computes the size of a grid coverage given its grid envelope and the target sample model
     * 
     * @param envelope
     * @param sm
     * @return
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
     * Computes the number of pixels for this {@link GridEnvelope2D}.
     * 
     * @param rasterEnvelope the {@link GridEnvelope2D} to compute the number of pixels for
     * @return the number of pixels for the provided {@link GridEnvelope2D}
     */
    private static long computePixelsNumber(GridEnvelope2D rasterEnvelope) {
        // pixels
        long pixelsNumber = 1;
        final int dimensions = rasterEnvelope.getDimension();
        for (int i = 0; i < dimensions; i++) {
            pixelsNumber *= rasterEnvelope.getSpan(i);
        }
        return pixelsNumber;
    }

    /**
     * Utility function to format a byte amount into a human readable string
     * 
     * @param bytes
     * @return
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
}