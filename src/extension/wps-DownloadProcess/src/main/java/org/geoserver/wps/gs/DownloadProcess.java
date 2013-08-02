/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;

import org.apache.commons.io.FilenameUtils;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.platform.GeoServerExtensions;
import org.geoserver.wfs.response.ShapeZipOutputFormat;
import org.geoserver.wps.WPSClusterStorageCleaner;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.WPSInfo;
import org.geoserver.wps.executor.ClusterProcessManager.ClusterProcessListener;
import org.geoserver.wps.gs.utils.LimitedFileOutputStream;
import org.geoserver.wps.mail.SendMail;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geoserver.wps.ppio.WFSPPIO;
import org.geoserver.wps.ppio.ZipArchivePPIO;
import org.geoserver.wps.ppio.ZipArchivePPIO.ZipArchive;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.imageio.GeoToolsWriteParams;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.feature.gs.ClipProcess;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.Intersects;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

/**
 * The Class DownloadProcess.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@DescribeProcess(title = "Enterprise Download Process", description = "Downloads Layer Stream and provides a ZIP.")
public class DownloadProcess extends AbstractDownloadProcess {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logging.getLogger(DownloadProcess.class);

    /** The estimator. */
    DownloadEstimatorProcess estimator;

    /** The send mail. */
    private SendMail sendMail;

    /** The storage cleaner */
    private WPSClusterStorageCleaner cleaner;

    /**
     * Instantiates a new download process.
     * 
     * @param geoServer the geo server
     * @param sendMail the send mail
     * @param estimator the estimator
     */
    public DownloadProcess(GeoServer geoServer, SendMail sendMail,
            DownloadEstimatorProcess estimator, WPSClusterStorageCleaner cleaner) {
        super(geoServer);
        this.sendMail = sendMail;
        this.estimator = estimator;
        this.cleaner = cleaner;
    }

    /**
     * Execute.
     * 
     * @param layerName the layer name
     * @param filter the filter
     * @param email the email
     * @param outputFormat the output format
     * @param targetCRS the target crs
     * @param roi the roi
     * @param cropToGeometry the crop to geometry
     * @param progressListener the progress listener
     * @return the file
     * @throws ProcessException the process exception
     */
    @DescribeResult(name = "result", description = "Zipped output files to download")
    public File execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vectorial Filter") String filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 0, description = "Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "RoiCRS", min = 0, description = "Region Of Interest CRS") CoordinateReferenceSystem roiCRS,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            final ProgressListener progressListener) throws ProcessException {

        if (estimator != null)
            estimator.execute(layerName, filter, email, outputFormat, targetCRS, roiCRS, roi,
                    cropToGeometry, progressListener);

        if (layerName != null) {

            getLayerAndResourceInfo(layerName);

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
                sendMail(email, progressListener, null, true);

                return handleVectorialLayerDownload(filter, email, outputFormat, targetCRS, roiCRS,
                        roi, cropToGeometry, progressListener);

            } else if (storeInfo instanceof CoverageStoreInfo) {
                sendMail(email, progressListener, null, true);

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

                        if (estimator != null && estimator.getGc() != null
                                && estimator.getFinalCoverage() != null) {
                            setGc(estimator.getGc());
                            setFinalCoverage(estimator.getFinalCoverage());
                        } else {
                            getCoverage(coverage, roi, roiCRS, targetCRS, cropToGeometry,
                                    progressListener);
                        }

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
                        long limit = (estimator != null && estimator.getHardOutputLimit() > 0 ? estimator
                                .getHardOutputLimit() * 1024
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

                        writeRasterOutput(outputFormat, progressListener, extension, os);

                        ZipArchive ppio = new ZipArchivePPIO.ZipArchive(geoServer, null);

                        File tempZipFile = new File(FilenameUtils.getFullPath(output
                                .getAbsolutePath()), FilenameUtils.getBaseName(output.getName())+ ".zip");
                        ppio.encode(output, new FileOutputStream(tempZipFile));

                        if (this.cleaner != null) {
                            long now = System.currentTimeMillis();
                            this.cleaner.lock(output);
                            this.cleaner.lock(tempZipFile);
                            this.cleaner.scheduleForCleaning(
                                    ((ClusterProcessListener) progressListener).getStatus()
                                            .getExecutionId(), now);
                        }

                        sendMail(email, progressListener, tempZipFile, false);

                        if (progressListener != null) {
                            progressListener.complete();
                        }

                        return tempZipFile;
                    } catch (Exception e) {
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

    /**
     * Write raster output.
     * 
     * @param outputFormat the output format
     * @param progressListener the progress listener
     * @param extension the extension
     * @param os the os
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void writeRasterOutput(String outputFormat, final ProgressListener progressListener,
            String extension, OutputStream os) throws IOException {
        try {
            if (extension.toLowerCase().contains("tif")) {

                GeoTiffWriter gtiffWriter = new GeoTiffWriter(os);

                try {
                    final ParameterValue<GeoToolsWriteParams> gtWparam = AbstractGridFormat.GEOTOOLS_WRITE_PARAMS
                            .createValue();
                    GeoTiffWriteParams param = new GeoTiffWriteParams();
                    gtWparam.setValue(param);
                    GeneralParameterValue[] params = new GeneralParameterValue[] { gtWparam };

                    gtiffWriter.write(getFinalCoverage(), params);
                } finally {
                    gtiffWriter.dispose();
                }
            } else {
                Iterator<ImageWriter> imageWriter = ImageIO.getImageWritersByFormatName(extension);
                if (imageWriter == null) {
                    imageWriter = ImageIO.getImageWritersByMIMEType(outputFormat);
                }

                if (imageWriter != null) {
                    ImageWriter writer = imageWriter.next();
                    try {
                        writer.setOutput(ImageIO.createImageOutputStream(os));
                        writer.write(getFinalCoverage().getRenderedImage());
                    } finally {
                        writer.dispose();
                    }
                } else {
                    cause = new IllegalStateException(
                            "Could not find a writer for the specified outputFormat!");
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(cause));
                    }
                    throw new ProcessException(cause);
                }
            }
        } finally {
            getGc().dispose(true);
            getFinalCoverage().dispose(true);
        }
    }

    /**
     * Handle vectorial layer download.
     * 
     * @param filter the filter
     * @param email the email
     * @param outputFormat the output format
     * @param targetCRS the target crs
     * @param roi the roi
     * @param cropToGeometry the crop to geometry
     * @param progressListener the progress listener
     * @return the file
     */
    private File handleVectorialLayerDownload(String filter, String email, String outputFormat,
            CoordinateReferenceSystem targetCRS, CoordinateReferenceSystem roiCRS, Geometry roi,
            Boolean cropToGeometry, final ProgressListener progressListener) {
        final DataStoreInfo dataStore = (DataStoreInfo) storeInfo;

        Filter ra = null;
        SimpleFeatureSource featureSource = null;
        try {
            // === filter or expression
            if (filter != null) {
                try {
                    ra = ECQL.toFilter(filter);
                } catch (Exception e) {
                    cause = new WPSException("Unable to parse input expression", e);
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(cause));
                    }
                    throw new ProcessException(cause);
                }
            } else {
                ra = Filter.INCLUDE;
            }

            if (estimator != null && estimator.getFeatureSource() != null)
                featureSource = estimator.getFeatureSource();
            else
                featureSource = getFeatureSource(dataStore, progressListener);

            SimpleFeatureCollection features = featureSource.getFeatures(ra);
            boolean needResample = false;
            String srs = null;
            CoordinateReferenceSystem referenceCRS;
            if (roiCRS == null || features.getSchema().getCoordinateReferenceSystem() != null) {
                referenceCRS = features.getSchema().getCoordinateReferenceSystem();
            } else {
                referenceCRS = roiCRS;
            }
            srs = CRS.toSRS(referenceCRS);
            ReferencedEnvelope refEnvelope = null;

            if (roi != null) {
                roi = JTS.transform(roi, CRS.findMathTransform(roiCRS, referenceCRS));

                final com.vividsolutions.jts.geom.Envelope envelope = roi.getEnvelopeInternal();

                refEnvelope = new ReferencedEnvelope(envelope, referenceCRS);
            } else {
                refEnvelope = features.getBounds();
            }

            // reproject the feature envelope if needed
            MathTransform targetTX = null;
            if (targetCRS != null) {
                if (!CRS.equalsIgnoreMetadata(referenceCRS, targetCRS)) {

                    // testing reprojection...
                    try {
                        /* if (! ( */targetTX = CRS.findMathTransform(referenceCRS, targetCRS) /*
                                                                                                * instanceof AffineTransform) ) throw new
                                                                                                * ProcessException
                                                                                                * ("Could not reproject to reference CRS")
                                                                                                */;
                    } catch (Exception e) {
                        if (progressListener != null) {
                            progressListener.exceptionOccurred(new ProcessException(
                                    "Could not reproject to reference CRS", e));
                        }
                        throw new ProcessException("Could not reproject to reference CRS", e);
                    }

                    refEnvelope = refEnvelope.transform(targetCRS, true);
                    needResample = true;
                }
            }

            // ---- START - Envelope and geometry sanity checks
            if ((refEnvelope.getLowerCorner().getOrdinate(0) == refEnvelope.getUpperCorner()
                    .getOrdinate(0))
                    || (refEnvelope.getLowerCorner().getOrdinate(1) == refEnvelope.getUpperCorner()
                            .getOrdinate(1))) {
                if (progressListener != null) {
                    progressListener
                            .exceptionOccurred(new ProcessException(
                                    "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!"));
                }
                throw new ProcessException(
                        "Reference CRS is not valid for this projection. Destination envelope has 0 dimension!");
            }

            // Geometry clipGeometry = roi.intersection(JTS.toGeometry(features.getBounds()));
            Geometry clipGeometry = roi;

            if (clipGeometry != null) {
                if (clipGeometry instanceof Point || clipGeometry instanceof MultiPoint) {
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "The Region of Interest is not a valid geometry!"));
                    }
                    throw new ProcessException("The Region of Interest is not a valid geometry!");
                }
            }
            // ---- END - Envelope and geometry sanity checks

            if (clipGeometry != null && (cropToGeometry == null || cropToGeometry)) {
                ClipProcess clip = new ClipProcess();
                features = clip.execute(features, clipGeometry);
            } else {
                // only get the geometries in the bbox of the clip
                FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

                if (clipGeometry != null) {
                    String dataGeomName = features.getSchema().getGeometryDescriptor()
                            .getLocalName();
                    Intersects intersectionFilter = ff.intersects(ff.property(dataGeomName),
                            ff.literal(clipGeometry));
                    features = features.subCollection(intersectionFilter);
                }
            }

            if (features == null || features.isEmpty()) {
                cause = new WPSException("Got an empty feature collection.");
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(cause));
                }
                throw new ProcessException(cause);
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
            final File output = File.createTempFile(resourceInfo.getName(), "." + extension,
                    getWpsOutputStorage());
            long limit = (estimator != null && estimator.getHardOutputLimit() > 0 ? estimator
                    .getHardOutputLimit() * 1024 : (estimator != null
                    && estimator.getWriteLimits() > 0 ? estimator.getWriteLimits() * 1024
                    : Long.MAX_VALUE));
            OutputStream os = new LimitedFileOutputStream(new FileOutputStream(output), limit) {

                @Override
                protected void raiseError(long pSizeMax, long pCount) throws IOException {
                    IOException e = new IOException(
                            "Download Exceeded the maximum HARD allowed size!");
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(e));
                    }
                    throw e;
                }

            };

            writeVectorialOutput(outputFormat, progressListener, features, extension, os);

            ZipArchive ppio = new ZipArchivePPIO.ZipArchive(geoServer, null);

            File tempZipFile = new File(FilenameUtils.getFullPath(output.getAbsolutePath()),
                    FilenameUtils.getBaseName(output.getName()) + ".zip");
            ppio.encode(output, new FileOutputStream(tempZipFile));

            if (this.cleaner != null) {
                long now = System.currentTimeMillis();
                this.cleaner.lock(output);
                this.cleaner.lock(tempZipFile);
                this.cleaner.scheduleForCleaning(
                        ((ClusterProcessListener) progressListener).getStatus()
                                .getExecutionId(), now);
            }

            sendMail(email, progressListener, tempZipFile, false);

            if (progressListener != null) {
                progressListener.complete();
            }

            return tempZipFile;
        } catch (Exception e) {
            cause = e;
            if (progressListener != null) {
                progressListener.exceptionOccurred(new ProcessException(cause));
            }
            throw new ProcessException(cause);
        }
    }

    /**
     * Write vectorial output.
     * 
     * @param outputFormat the output format
     * @param progressListener the progress listener
     * @param features the features
     * @param extension the extension
     * @param os the os
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws Exception the exception
     */
    private void writeVectorialOutput(String outputFormat, final ProgressListener progressListener,
            SimpleFeatureCollection features, String extension, OutputStream os)
            throws IOException, Exception {
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
                        progressListener.exceptionOccurred(new ProcessException(cause));
                    }
                    throw new ProcessException(cause);
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
    }

    /**
     * Send mail.
     * 
     * @param email the email
     * @param progressListener the progress listener
     * @param output
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void sendMail(String email, final ProgressListener progressListener, File output,
            boolean started) {
        if (email != null && sendMail != null) {
            if (progressListener != null && progressListener instanceof ClusterProcessListener) {
                try {
                    if (started) {
                        sendMail.sendStartedNotification(email,
                                ((ClusterProcessListener) progressListener).getStatus()
                                        .getExecutionId());

                    } else {
                        
                        // handle the resource expiration timeout
                        WPSInfo info = geoServer.getService(WPSInfo.class);
                        double timeout = info.getResourceExpirationTimeout();
                        int expirationDelay = -1;
                        if (timeout > 0) {
                            expirationDelay = ((int) timeout * 1000);
                        } else {
                            // specified timeout == -1, so we use the default of five minutes
                            expirationDelay = (5 * 60 * 1000);
                        }
                        
                        sendMail.sendFinishedNotification(email,
                                ((ClusterProcessListener) progressListener).getStatus()
                                        .getExecutionId(), output, expirationDelay);
                    }
                } catch (Exception e) {
                    LOGGER.warning("Could not send the notification email : "
                            + e.getLocalizedMessage());
                }
            }
        }
    }
}