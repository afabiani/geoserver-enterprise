/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.awt.Color;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.image.ColorModel;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.io.File;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.servlet.ServletContext;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wfs.response.ShapeZipOutputFormat;
import org.geoserver.wps.WPSException;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
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
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;
import org.jaitools.imageutils.ImageLayout2;
import org.opengis.coverage.ColorInterpretation;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory;
import org.opengis.geometry.Envelope;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;
import org.vfny.geoserver.wcs.WcsException;

import com.sun.media.jai.opimage.RIFUtil;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

/**
 * 
 * @author Alessio Fabiani
 * 
 */
public abstract class AbstractDownloadProcess implements GSProcess {

    /**
     * 
     */
    protected static final Logger LOGGER = Logging.getLogger(AbstractDownloadProcess.class);

    /**
     * 
     */
    protected static GridCoverageFactory gcFactory = new GridCoverageFactory();

    /**
     * 
     */
    protected GeoServer geoServer;

    /**
     * 
     */
    protected Catalog catalog;

    /**
     * 
     */
    protected FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    /**
     * 
     */
    protected GeometryBuilder geomBuilder = new GeometryBuilder();

    /**
     * 
     */
    protected Throwable cause = null;

    /**
     * 
     */
    protected LayerInfo layerInfo = null;

    /**
     * 
     */
    protected ResourceInfo resourceInfo = null;

    /**
     * 
     */
    protected StoreInfo storeInfo = null;

    /**
     * 
     */
    private SimpleFeatureSource featureSource = null;

    /**
     * 
     */
    private GridCoverage2D gc = null;

    /**
     * 
     */
    private GridCoverage2D finalCoverage = null;

    /**
     * 
     * @param geoServer
     */
    public AbstractDownloadProcess(GeoServer geoServer) {
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
    }

    /**
     * @param featureSource the featureSource to set
     */
    public void setFeatureSource(SimpleFeatureSource featureSource) {
        this.featureSource = featureSource;
    }

    /**
     * @return the featureSource
     */
    public SimpleFeatureSource getFeatureSource() {
        return featureSource;
    }

    /**
     * 
     * @param layerName
     * @param filter
     * @param email
     * @param outputFormat
     * @param targetCRS
     * @param roi
     * @param cropToGeometry
     * @param progressListener
     * @return
     * @throws ProcessException
     */
    @DescribeResult(name = "result", description = "Zipped output files to download")
    public abstract Object execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vectorial Filter") String filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 1, description = "Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "ROI", min = 1, description = "Region Of Interest") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            final ProgressListener progressListener) throws ProcessException;

    /**
     * 
     * @param layerName
     */
    protected void getLayerAndResourceInfo(String layerName) {
        if (layerInfo == null || resourceInfo == null || storeInfo == null) {
            layerInfo = catalog.getLayerByName(layerName);
            resourceInfo = layerInfo.getResource();
            storeInfo = resourceInfo.getStore();
        }
    }

    /**
     * 
     * @param dataStore
     * @return
     * @throws Exception
     */
    protected SimpleFeatureSource getFeatureSource(DataStoreInfo dataStore, ProgressListener progressListener) throws Exception {
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
        return ds.getFeatureSource(targetType.getTypeName());
    }

    /**
     * 
     * @param coverage
     * @param roi
     * @param targetCRS
     * @param gc
     * @param finalCoverage
     * @param progressListener
     * @throws Exception
     */
    protected void getCoverage(CoverageInfo coverage, Geometry roi,
            CoordinateReferenceSystem targetCRS, ProgressListener progressListener)
            throws Exception {
        // pixel scale
        final MathTransform tempTransform = coverage.getGrid().getGridToCRS();
        if (!(tempTransform instanceof AffineTransform)) {
            cause = new IllegalArgumentException(
                    "Grid to world tranform is not an AffineTransform:" + resourceInfo.getName());
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(
                        "Could not complete the Download Process", cause));
            }
            throw new ProcessException("Could not complete the Download Process", cause);
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
            final com.vividsolutions.jts.geom.Envelope envelope = roi.getEnvelopeInternal();

            ReferencedEnvelope refEnvelope = new ReferencedEnvelope(envelope, targetCRS);

            // reproject the coverage envelope if needed
            if (!CRS.equalsIgnoreMetadata(targetCRS, referenceCRS)) {

                // testing reprojection...
                try {
                    /* if (! ( */CRS.findMathTransform(targetCRS, referenceCRS) /*
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

                refEnvelope = refEnvelope.transform(referenceCRS, true);
                needResample = true;
            }

            finalEnvelope = new ReferencedEnvelope(refEnvelope.intersection(finalEnvelope),
                    referenceCRS);
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
            throw new ProcessException("Could not complete the Download Process", cause);
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

        Geometry cropShape = roi;
        ReferencedEnvelope finalEnvelopeInTargetCRS = finalEnvelope.transform(targetCRS, true);
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
        // ---- END - Envelope and geometry sanity checks

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
        gc = (GridCoverage2D) coverage.getGridCoverageReader(new NullProgressListener(), hints)
                .read(new GeneralParameterValue[] { streamingRead, readGG, suggestedTileSize });

        // create return coverage reusing origin grid to world
        RenderedImage raster = null;
        if (needResample) {
            hints.add(new RenderingHints(JAI.KEY_INTERPOLATION, Interpolation
                    .getInstance(Interpolation.INTERP_NEAREST)));
            ReferencedEnvelope targetEnvelope = finalEnvelope.transform(targetCRS, true);
            GridCoverageRenderer renderer = new GridCoverageRenderer(targetCRS, targetEnvelope, gc
                    .getGridGeometry().getGridRange2D().getBounds(), (AffineTransform) null, hints);
            raster = renderer.renderImage(gc, new RasterSymbolizerImpl(),
                    Interpolation.getInstance(Interpolation.INTERP_NEAREST), Color.WHITE,
                    Integer.parseInt(suggestedTileSize.getValue().split(",")[1]),
                    Integer.parseInt(suggestedTileSize.getValue().split(",")[0]));

            finalCoverage = createCoverage("resampled", raster, targetEnvelope);
        } else {
            finalCoverage = gc;
        }

        // ---- Cropping coverage to the Region of Interest
        CropCoverage cropProcess = new CropCoverage();
        finalCoverage = cropProcess.execute(finalCoverage, cropShape, progressListener);
    }

    /**
     * 
     * @return
     */
    public static Charset getCharset() {
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
     * 
     * @return
     */
    public static File getWpsOutputStorage() {
        File wpsStore = null;
        try {
            String wpsOutputStorage = GeoServerExtensions.getProperty("WPS_OUTPUT_STORAGE");
            File temp = null;
            if (wpsOutputStorage == null || !new File(wpsOutputStorage).exists())
                temp = GeoserverDataDirectory.findCreateConfigDir("temp");
            else {
                temp = new File(wpsOutputStorage);
            }
            wpsStore = new File(temp, "wps");
            if (!wpsStore.exists()) {
                mkdir(wpsStore);
            }
        } catch (Exception e) {
            throw new WcsException("Could not create the temporary storage directory for WPS");
        }
        return wpsStore;
    }

    /**
     * 
     * @param file
     */
    public static void mkdir(File file) {
        if (!file.mkdir()) {
            throw new WPSException("Failed to create the specified directory " + file);
        }
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
    public static long getCoverageSize(GridEnvelope2D envelope, SampleModel sm) {
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
    public static long computePixelsNumber(GridEnvelope2D rasterEnvelope) {
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
    public static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        } else if (bytes < 1024 * 1024) {
            return new DecimalFormat("#.##").format(bytes / 1024.0) + "KB";
        } else {
            return new DecimalFormat("#.##").format(bytes / 1024.0 / 1024.0) + "MB";
        }
    }

    /**
     * @param gc the gc to set
     */
    public void setGc(GridCoverage2D gc) {
        this.gc = gc;
    }

    /**
     * @return the gc
     */
    public GridCoverage2D getGc() {
        return gc;
    }

    /**
     * @param finalCoverage the finalCoverage to set
     */
    public void setFinalCoverage(GridCoverage2D finalCoverage) {
        this.finalCoverage = finalCoverage;
    }

    /**
     * @return the finalCoverage
     */
    public GridCoverage2D getFinalCoverage() {
        return finalCoverage;
    }
}