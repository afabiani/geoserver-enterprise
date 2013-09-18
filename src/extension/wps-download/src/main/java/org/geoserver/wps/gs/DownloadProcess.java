/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.ppio.ZipArchivePPIO;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.util.Utilities;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.ProgressListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * The Class DownloadProcess.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@DescribeProcess(title = "Enterprise Download Process", description = "Downloads Layer Stream and provides a ZIP.")
public class DownloadProcess implements GSProcess {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logging.getLogger(DownloadProcess.class);

    /** The estimator. */
    private final DownloadEstimatorProcess estimator;

    /** The catalog. */
    private final Catalog catalog;

    private ZipArchivePPIO zipPPIO;

    /**
     * Instantiates a new download process.
     * 
     * @param geoServer the geo server
     * @param sendMail the send mail
     * @param estimator the estimator
     */
    public DownloadProcess(GeoServer geoServer, DownloadEstimatorProcess estimator,
            ZipArchivePPIO zipPPIO) {
        Utilities.ensureNonNull("geoServer", geoServer);
        this.catalog = geoServer.getCatalog();
        this.estimator = estimator;
        this.zipPPIO = zipPPIO;
    }

    /**
     * Execute.
     * 
     * @param layerName the layer name
     * @param filter the filter
     * @param email the email
     * @param mimeType the output format
     * @param targetCRS the target crs
     * @param roiCRS the roi crs
     * @param roi the roi
     * @param clip the crop to geometry
     * @param progressListener the progress listener
     * @return the file
     * @throws ProcessException the process exception
     */
    @DescribeResult(name = "result", description = "Zipped output files to download")
    public File execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vector Filter") Filter filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format Mime-Type") String mimeType,
            @DescribeParameter(name = "targetCRS", min = 0, description = "Optional Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "RoiCRS", min = 0, description = "Optional Region Of Interest CRS") CoordinateReferenceSystem roiCRS,
            @DescribeParameter(name = "ROI", min = 0, description = "Optional Region Of Interest (Polygon)") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean clip,
            final ProgressListener progressListener) throws ProcessException {

        try {
            //
            // initial checks on mandatory params
            //
            // layer name
            if (layerName == null || layerName.length() <= 0) {
                throw new IllegalArgumentException("Empty or null layerName provided!");
            }
            if (clip == null) {
                clip = false;
            }
            if (roi != null) {
                DownloadUtilities.checkPolygonROI(roi);
                if (roiCRS == null) {
                    throw new IllegalArgumentException("ROI without a CRS is not usable!");
                }
                roi.setUserData(roiCRS);
            }

            //
            // do we respect limits?
            //
            if (!estimator.execute(layerName, filter, targetCRS, roiCRS, roi, clip,
                    progressListener)) {
                throw new IllegalArgumentException("Download Limits Exceeded. Unable to proceed!");
            }

            //
            // Move on with the real code
            //
            // cheking for the rsources on the GeoServer catalog
            LayerInfo layerInfo = catalog.getLayerByName(layerName);
            if (layerInfo == null) {
                // could not find any layer ... abruptly interrupt the process
                throw new IllegalArgumentException("Unable to locate layer: " + layerName);

            }
            ResourceInfo resourceInfo = layerInfo.getResource();
            if (resourceInfo == null) {
                // could not find any data store associated to the specified layer ... abruptly interrupt the process
                throw new IllegalArgumentException("Unable to locate ResourceInfo for layer:"
                        + layerName);

            }

            // CORE CODE
            File output;
            if (resourceInfo instanceof FeatureTypeInfo) {
                //
                // VECTOR
                //
                // perform the actual download of vectorial data accordingly to the request inputs
                output = new VectorDownload(estimator).execute((FeatureTypeInfo) resourceInfo,
                        mimeType, roi, clip, filter, targetCRS, progressListener);

            } else if (resourceInfo instanceof CoverageInfo) {
                //
                // RASTER
                //
                CoverageInfo cInfo = (CoverageInfo) resourceInfo;
                // convert/reproject/crop if needed the coverage
                output = new RasterDownload(estimator).execute(mimeType, progressListener, cInfo,
                        roi, targetCRS, clip, filter);
            } else {

                // wrong type
                throw new IllegalArgumentException(
                        "Could not complete the Download Process, requested layer was of wrong type-->"
                                + resourceInfo.getClass());

            }

            //
            // Work on result
            //
            // checks
            if (output == null) {
                // wrong type
                throw new IllegalStateException(
                        "Could not complete the Download Process, output file is null");
            }
            if (!output.exists() || !output.canRead()) {
                // wrong type
                throw new IllegalStateException(
                        "Could not complete the Download Process, output file invalid! --> "
                                + output.getAbsolutePath());

            }

            // zipping and adding the style

            // build destination zip
            final File newOutput = File.createTempFile(FilenameUtils.getBaseName(output.getName()),
                    ".zip", output.getParentFile());

            FileOutputStream os1 = null;
            try {
                os1 = new FileOutputStream(newOutput);

                // add old output and default SLD to zip
                File style = GeoserverDataDirectory.findStyleFile(layerInfo.getDefaultStyle().getFilename());
                
                List<File> filesToDownload = null; 
                if (style != null && style.exists() && style.canRead()) filesToDownload = Arrays.asList(style, output);
                else filesToDownload = Arrays.asList(output);
                
                zipPPIO.encode(filesToDownload, os1);

            } finally {
                if (os1 != null) {
                    IOUtils.closeQuietly(os1);
                }

                // delete original
                if (output.isDirectory()) {
                    org.geoserver.data.util.IOUtils.delete(output);
                } else {
                    FileUtils.deleteQuietly(output);
                }
            }

            output = newOutput; // reassign

            //
            // finishing
            //
            // Completed!
            if (progressListener != null) {
                progressListener.complete();
            }

            // return
            return output;
        } catch (Exception e) {
            // catch and rethrow
            final ProcessException processException = new ProcessException(e);
            if (progressListener != null) {
                progressListener.exceptionOccurred(processException);
            }
            throw processException;
        }
    }
}