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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.mail.MessagingException;
import javax.media.jai.ImageLayout;
import javax.media.jai.Interpolation;
import javax.media.jai.JAI;
import javax.servlet.ServletContext;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wfs.response.ShapeZipOutputFormat;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.executor.ClusterProcessManager.ClusterProcessListener;
import org.geoserver.wps.gs.utils.LimitedFileOutputStream;
import org.geoserver.wps.mail.SendMail;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geoserver.wps.ppio.WFSPPIO;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.InvalidGridGeometryException;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.imageio.GeoToolsWriteParams;
import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.factory.Hints;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.GeneralEnvelope;
import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.feature.gs.ClipProcess;
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
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.Intersects;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.ProgressListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;
import org.vfny.geoserver.wcs.WcsException;

import com.sun.media.jai.opimage.RIFUtil;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

@DescribeProcess(title = "Enterprise Download Process", description = "Downloads Layer Stream and provides a ZIP.")
public class DownloadProcess implements GSProcess {

    protected static final Logger LOGGER = Logging.getLogger(DownloadProcess.class);

    private static GridCoverageFactory gcFactory = new GridCoverageFactory();

    protected GeoServer geoServer;

    protected Catalog catalog;

    protected FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    protected GeometryBuilder geomBuilder = new GeometryBuilder();

    protected SendMail sendMail;

    DownloadEstimatorProcess estimator;

    private long hardOutputLimit;

    public DownloadProcess(GeoServer geoServer, SendMail sendMail,
            DownloadEstimatorProcess estimator) {
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
        this.sendMail = sendMail;
        this.estimator = estimator;
        this.hardOutputLimit = 0;
    }

    /**
     * @param hardOutputLimit the hardOutputLimit to set
     */
    public void setHardOutputLimit(long hardOutputLimit) {
        this.hardOutputLimit = hardOutputLimit;
    }

    /**
     * @return the hardOutputLimit
     */
    public long getHardOutputLimit() {
        return hardOutputLimit;
    }

    @DescribeResult(name = "result", description = "Zipped output files to download")
    public File execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vectorial Filter") String filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 1, description = "Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "ROI", min = 1, description = "Region Of Interest") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            final ProgressListener progressListener) throws ProcessException {

        if (estimator != null)
            estimator.execute(layerName, filter, email, outputFormat, targetCRS, roi,
                    cropToGeometry, progressListener);

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
                    boolean needResample = false;
                    String srs = null;
                    CoordinateReferenceSystem referenceCRS;
                    if (features.getSchema().getCoordinateReferenceSystem() != null) {
                        referenceCRS = features.getSchema().getCoordinateReferenceSystem();
                    } else {
                        referenceCRS = targetCRS;
                    }
                    srs = CRS.toSRS(referenceCRS);

                    final com.vividsolutions.jts.geom.Envelope envelope = roi.getEnvelopeInternal();

                    ReferencedEnvelope refEnvelope = new ReferencedEnvelope(envelope, targetCRS);

                    // reproject the coverage envelope if needed
                    if (!CRS.equalsIgnoreMetadata(targetCRS, referenceCRS)) {

                        // testing reprojection...
                        try {
                            /* if (! ( */CRS.findMathTransform(targetCRS, referenceCRS) /*
                                                                                         * instanceof AffineTransform) ) throw new
                                                                                         * ProcessException("Could not reproject to reference CRS")
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

                    // ---- START - Envelope and geometry sanity checks
                    if ((refEnvelope.getLowerCorner().getOrdinate(0) == refEnvelope
                            .getUpperCorner().getOrdinate(0))
                            || (refEnvelope.getLowerCorner().getOrdinate(1) == refEnvelope
                                    .getUpperCorner().getOrdinate(1))) {
                        if (progressListener != null) {
                            progressListener
                                    .exceptionOccurred(new ProcessException(
                                            "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!"));
                        }
                        throw new ProcessException(
                                "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!");
                    }
                    Geometry clipGeometry = (needResample ? JTS.transform(roi,
                            CRS.findMathTransform(targetCRS, referenceCRS)) : roi);
                    // clipGeometry = clipGeometry.intersection(other);

                    if (clipGeometry instanceof Point || clipGeometry instanceof MultiPoint) {
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "The Region of Interest is not a valid geometry!"));
                        }
                        throw new ProcessException(
                                "The Region of Interest is not a valid geometry!");
                    }
                    // ---- END - Envelope and geometry sanity checks

                    if (cropToGeometry == null || cropToGeometry) {
                        ClipProcess clip = new ClipProcess();
                        features = clip.execute(features, clipGeometry);
                    } else {
                        // only get the geometries in the bbox of the clip
                        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

                        // BBOX bboxFilter = ff.bbox("", refEnvelope.getMinX(), refEnvelope.getMinY(), refEnvelope.getMaxX(), refEnvelope.getMaxY(),
                        // srs);
                        // features = features.subCollection(bboxFilter);

                        String dataGeomName = features.getSchema().getGeometryDescriptor()
                                .getLocalName();
                        Intersects intersectionFilter = ff.intersects(ff.property(dataGeomName),
                                ff.literal(clipGeometry));
                        features = features.subCollection(intersectionFilter);
                    }

                    if (features == null || features.isEmpty()) {
                        cause = new WPSException("Got an empty feature collection.");
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not complete the Download Process", cause));
                        }
                        throw new ProcessException("Could not complete the Download Process", cause);
                    }

                    if (needResample) {
                        features = new ReprojectingFeatureCollection(features, targetCRS);
                    }

                    String extension = null;
                    if (outputFormat.toLowerCase().contains("shape")
                            || outputFormat.toLowerCase().contains("shp")) {
                        extension = "zip";
                    } else if (outputFormat.toLowerCase().contains("json")) {
                        extension = "json";
                    } else if (outputFormat.toLowerCase().contains("dxf")) {
                        extension = "dxf";
                    } else {
                        extension = "xml";
                    }

                    // writing the output
                    final File output = File.createTempFile(resourceInfo.getName(),
                            "." + extension, getWpsOutputStorage());
                    long limit = (hardOutputLimit > 0 ? hardOutputLimit * 1024 : (estimator != null
                            && estimator.getWriteLimits() > 0 ? estimator.getWriteLimits() * 1024
                            : Long.MAX_VALUE));
                    OutputStream os = new LimitedFileOutputStream(new FileOutputStream(output),
                            limit) {

                        @Override
                        protected void raiseError(long pSizeMax, long pCount) throws IOException {
                            IOException e = new IOException(
                                    "Download Exceeded the maximum HARD allowed size!");
                            if (progressListener != null) {
                                progressListener.exceptionOccurred(new ProcessException(
                                        "Could not complete the Download Process", e));
                            }
                            throw e;
                        }

                    };

                    try {
                        if ("zip".equals(extension)) {
                            ShapeZipOutputFormat of = new ShapeZipOutputFormat();
                            of.write(Collections.singletonList(features), getCharset(), os, null);
                        } else if ("json".equals(extension)) {
                            FeatureJSON json = new FeatureJSON();
                            // commented out due to GEOT-3209
                            // json.setEncodeFeatureCRS(true);
                            // json.setEncodeFeatureCollectionCRS(true);
                            json.writeFeatureCollection(features, os);
                        } else if (extension.toLowerCase().contains("dxf")) {
                            boolean encoded = false;
                            List<ProcessParameterIO> ppioextensions = GeoServerExtensions
                                    .extensions(ProcessParameterIO.class);
                            for (ProcessParameterIO ppio : ppioextensions) {
                                if (ppio instanceof ComplexPPIO
                                        && "dxf".equals(((ComplexPPIO) ppio).getFileExtension())) {
                                    ((ComplexPPIO) ppio).encode(features, os);
                                    encoded = true;
                                }
                            }

                            if (!encoded) {
                                cause = new WPSException("DXF Extension is not available.");
                                if (progressListener != null) {
                                    progressListener.exceptionOccurred(new ProcessException(
                                            "Could not complete the Download Process", cause));
                                }
                                throw new ProcessException(
                                        "Could not complete the Download Process", cause);
                            }
                        } else {
                            /**
                             * application/gml-2.1.2 application/gml-3.1.1 application/wfs-collection-1.0 application/wfs-collection-1.1
                             */
                            if (outputFormat.toLowerCase().contains("gml-3.1.1")
                                    || outputFormat.toLowerCase().contains("wfs-collection-1.1")
                                    || outputFormat.toLowerCase().contains("wfs-collection/1.1"))
                                new WFSPPIO.WFS11().encode(features, os);
                            else
                                new WFSPPIO.WFS10().encode(features, os);
                        }
                    } finally {
                        os.close();
                    }

                    if (email != null && sendMail != null) {
                        if (progressListener != null
                                && progressListener instanceof ClusterProcessListener) {
                            try {
                                sendMail.sendFinishedNotification(email,
                                        ((ClusterProcessListener) progressListener).getStatus()
                                                .getExecutionId());
                            } catch (MessagingException e) {
                                LOGGER.warning("Could not send the notification email : "
                                        + e.getLocalizedMessage());
                            }
                        }
                    }

                    return output;
                } catch (Exception e) {
                    cause = e;
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Could not complete the Download Process", cause));
                    }
                    throw new ProcessException("Could not complete the Download Process", cause);
                }
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

                            finalCovergae = DownloadEstimatorProcess.createCoverage("resampled",
                                    raster, targetEnvelope);
                        } else {
                            finalCovergae = gc;
                        }

                        // ---- Cropping coverage to the Region of Interest
                        CropCoverage cropProcess = new CropCoverage();
                        finalCovergae = cropProcess.execute(finalCovergae, cropShape,
                                progressListener);
                        raster = finalCovergae.getRenderedImage();

                        String extension = null;
                        if (outputFormat.toLowerCase().startsWith("image")
                                || outputFormat.indexOf("/") > 0) {
                            extension = outputFormat.substring(outputFormat.indexOf("/") + 1,
                                    outputFormat.length());
                        } else {
                            extension = outputFormat;
                        }

                        // writing the output
                        final File output = File.createTempFile(resourceInfo.getName(), "."
                                + extension, getWpsOutputStorage());
                        long limit = (hardOutputLimit > 0 ? hardOutputLimit * 1024
                                : (estimator != null && estimator.getWriteLimits() > 0 ? estimator
                                        .getWriteLimits() * 1024 : Long.MAX_VALUE));
                        OutputStream os = new LimitedFileOutputStream(new FileOutputStream(output),
                                limit) {

                            @Override
                            protected void raiseError(long pSizeMax, long pCount)
                                    throws IOException {
                                IOException e = new IOException(
                                        "Download Exceeded the maximum HARD allowed size!");
                                if (progressListener != null) {
                                    progressListener.exceptionOccurred(new ProcessException(
                                            "Could not complete the Download Process", e));
                                }
                                throw e;
                            }

                        };
                        try {
                            if (extension.toLowerCase().contains("tif")) {

                                GeoTiffWriter gtiffWriter = new GeoTiffWriter(os);

                                try {
                                    final ParameterValue<GeoToolsWriteParams> gtWparam = AbstractGridFormat.GEOTOOLS_WRITE_PARAMS
                                            .createValue();
                                    GeoTiffWriteParams param = new GeoTiffWriteParams();
                                    gtWparam.setValue(param);
                                    GeneralParameterValue[] params = new GeneralParameterValue[] { gtWparam };

                                    gtiffWriter.write(finalCovergae, params);
                                } finally {
                                    gtiffWriter.dispose();
                                }
                            } else {
                                Iterator<ImageWriter> imageWriter = ImageIO
                                        .getImageWritersByFormatName(extension);
                                if (imageWriter == null) {
                                    imageWriter = ImageIO.getImageWritersByMIMEType(outputFormat);
                                }

                                if (imageWriter != null) {
                                    ImageWriter writer = imageWriter.next();
                                    try {
                                        writer.setOutput(ImageIO.createImageOutputStream(os));
                                        writer.write(finalCovergae.getRenderedImage());
                                    } finally {
                                        writer.dispose();
                                    }
                                } else {
                                    cause = new IllegalStateException(
                                            "Could not find a writer for the specified outputFormat!");
                                    if (progressListener != null) {
                                        progressListener.exceptionOccurred(new ProcessException(
                                                "Could not complete the Download Process", cause));
                                    }
                                    throw new ProcessException(
                                            "Could not complete the Download Process", cause);
                                }
                            }
                        } finally {
                            gc.dispose(true);
                            finalCovergae.dispose(true);
                        }

                        if (email != null && sendMail != null) {
                            if (progressListener != null
                                    && progressListener instanceof ClusterProcessListener) {
                                try {
                                    sendMail.sendFinishedNotification(email,
                                            ((ClusterProcessListener) progressListener).getStatus()
                                                    .getExecutionId());
                                } catch (MessagingException e) {
                                    LOGGER.warning("Could not send the notification email : "
                                            + e.getLocalizedMessage());
                                }
                            }
                        }

                        return output;
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

    private Charset getCharset() {
        final String charsetName = GeoServerExtensions.getProperty(
                ShapeZipOutputFormat.GS_SHAPEFILE_CHARSET, (ServletContext) null);
        if (charsetName != null) {
            return Charset.forName(charsetName);
        } else {
            // if not specified let's use the shapefile default one
            return Charset.forName("ISO-8859-1");
        }
    }

    File getWpsOutputStorage() {
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

    private void mkdir(File file) {
        if (!file.mkdir()) {
            throw new WPSException("Failed to create the specified directory " + file);
        }
    }
}