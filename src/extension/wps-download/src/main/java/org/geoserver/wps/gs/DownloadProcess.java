/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.WPSClusterStorageCleaner;
import org.geoserver.wps.WPSException;
import org.geoserver.wps.WPSInfo;
import org.geoserver.wps.executor.ClusterProcessManager.ClusterProcessListener;
import org.geoserver.wps.gs.utils.LimitedFileOutputStream;
import org.geoserver.wps.mail.SendMail;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geoserver.wps.ppio.ZipArchivePPIO;
import org.geoserver.wps.ppio.ZipArchivePPIO.ZipArchive;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.imageio.GeoToolsWriteParams;
import org.geotools.data.Parameter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.feature.gs.ClipProcess;
import org.geotools.referencing.CRS;
import org.geotools.resources.coverage.FeatureUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.filter.spatial.Intersects;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;

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
    private DownloadEstimatorProcess estimator;

    /** The send mail. */
    protected final SendMail sendMail;

    /**
     * Instantiates a new download process.
     * 
     * @param geoServer the geo server
     * @param sendMail the send mail
     * @param estimator the estimator
     */
    public DownloadProcess(GeoServer geoServer, SendMail sendMail,
            DownloadEstimatorProcess estimator) {
        super(geoServer);
        this.sendMail=sendMail;
        this.estimator = estimator;
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
     * @return the file
     * @throws ProcessException the process exception
     */
    @DescribeResult(name = "result", description = "Zipped output files to download")
    public File execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vector Filter") Filter filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format Mime-Type") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 0, description = "Optional Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "RoiCRS", min = 0, description = "Optional Region Of Interest CRS") CoordinateReferenceSystem roiCRS,
            @DescribeParameter(name = "ROI", min = 0, description = "Optional Region Of Interest (Polygon)") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            final ProgressListener progressListener) throws ProcessException {

        //
        // initial checks on mandatory params
        //
        // layer name
        if(layerName==null|| layerName.length()<=0){
            ProcessException ex = new ProcessException("Empty or null layerName provided!");
            if (progressListener != null) {
                progressListener.exceptionOccurred(ex);
            }       
            throw ex;
        }
        if(cropToGeometry==null){
            cropToGeometry=false;
        }
        if(roi!=null){
            DownloadUtilities.checkPolygonROI(roi);
        }
        
        //
        // do we respect limits?
        //
        estimator.execute(layerName, filter, email, outputFormat, targetCRS, roiCRS, roi, cropToGeometry, progressListener);

        
        // 
        // Move on with the real code
        // 
        // cheking for the rsources on the GeoServer catalog
        LayerInfo layerInfo = catalog.getLayerByName(layerName);
        if (layerInfo == null) {
            // could not find any layer ... abruptly interrupt the process
            final IllegalArgumentException cause = new IllegalArgumentException("Unable to locate layer: "+ layerName);
            final ProcessException processException = new ProcessException("Could not complete the Download Process", cause);
            if (progressListener != null) {
                progressListener.exceptionOccurred(processException);
            }
            throw processException;
        }        
        ResourceInfo resourceInfo = layerInfo.getResource();
        if (resourceInfo == null) {
            // could not find any data store associated to the specified layer ... abruptly interrupt the process
            final IllegalArgumentException cause = new IllegalArgumentException("Unable to locate ResourceInfo for layer:"+ layerName);
            final ProcessException processException = new ProcessException(
                    "Could not complete the Download Process", cause);
            if (progressListener != null) {
                progressListener.exceptionOccurred(processException);
            }
            throw processException;
        }

        StoreInfo storeInfo = resourceInfo.getStore();
        if (storeInfo == null) {
            // could not find any data store associated to the specified layer ... abruptly interrupt the process
            final IllegalArgumentException cause = new IllegalArgumentException("Unable to locate StoreInfo for layer:"+ layerName);
            final ProcessException processException = new ProcessException(
                    "Could not complete the Download Process", cause);
            if (progressListener != null) {
                progressListener.exceptionOccurred(processException);
            }
            throw processException;
        }
        

        // first of all if sendMail is enabled, send an email for the starting process ...
        sendMail(email, progressListener, null, true);
        
        Exception cause;
        // ////
        // 1. DataStore -> look for vectorial data download
        // 2. CoverageStore -> look for raster data download
        // ////
        if (storeInfo instanceof DataStoreInfo) {
            try{
                // perform the actual download of vectorial data accordingly to the request inputs
                return handleVector(
                        resourceInfo, 
                        (DataStoreInfo)storeInfo, 
                        filter, 
                        email,
                        outputFormat, 
                        targetCRS, 
                        roiCRS, 
                        roi, 
                        cropToGeometry, 
                        progressListener);
            } catch (Exception e) {
                // catch and rethrow
                final ProcessException processException = new ProcessException(e);
                if (progressListener != null) {
                    progressListener.exceptionOccurred(processException);
                }
                throw processException;
            }

        } else if (storeInfo instanceof CoverageStoreInfo) {

            // get the "coverageInfo" from the GeoServer catalog
            final CoverageStoreInfo coverageStore = (CoverageStoreInfo) storeInfo;
            final CoverageInfo coverage = catalog.getCoverageByName(resourceInfo.getName());

            if (coverageStore == null || coverage == null) {
                LOGGER.severe("Unable to locate coverage:"
                        + resourceInfo.getName());
                cause = new IllegalArgumentException("Unable to locate coverage:"
                        + resourceInfo.getName());
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(
                            "Could not complete the Download Process", cause));
                }
                throw new ProcessException("Could not complete the Download Process", cause);
            } else {
                try {
                    // read the source Grid Coverage "roi" area from the mass storage at the source resolution
                    GridCoverage2D gc = getCoverage(resourceInfo, coverage, roi, roiCRS,
                            targetCRS, cropToGeometry, progressListener);

                    // look for output extension. Tiff/tif/geotiff will be all threated as GeoTIFF
                    String extension = null;
                    if (outputFormat.toLowerCase().startsWith("image")
                            || outputFormat.indexOf("/") > 0) {
                        extension = outputFormat.substring(outputFormat.indexOf("/") + 1,
                                outputFormat.length());
                    } else {
                        extension = outputFormat;
                    }

                    // writing the output to a temporary folder
                    final File output = File.createTempFile(resourceInfo.getName(), "."
                            + extension, WPSClusterStorageCleaner.getWpsOutputStorage());
                    long limit = (estimator != null && estimator.getHardOutputLimit() > 0 ? estimator
                            .getHardOutputLimit() * 1024
                            : (estimator != null && estimator.getWriteLimits() > 0 ? estimator
                                    .getWriteLimits() * 1024 : Long.MAX_VALUE));
                    
                    // the limit ouutput stream will throw an exception if the process is trying to writr more than the max allowed bytes
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

                    // convert/reproject/crop if needed the coverage
                    writeRasterOutput(outputFormat, progressListener, extension, os,
                            resourceInfo, coverage, gc, roi, roiCRS, targetCRS, cropToGeometry);

                    File tempZipFile = output;

                    // zip the output if it's not already an zrchive 
                    if (!FilenameUtils.getExtension(tempZipFile.getName()).equalsIgnoreCase(
                            "zip")) {
                        ZipArchive ppio = new ZipArchivePPIO.ZipArchive(geoServer, null);
                        tempZipFile = new File(FilenameUtils.getFullPath(output
                                .getAbsolutePath()),
                                FilenameUtils.getBaseName(output.getName()) + ".zip");
                        ppio.encode(output, new FileOutputStream(tempZipFile));
                    }

                    // if enabled, send an email back for completion with the link to the resource
                    sendMail(email, progressListener, tempZipFile, false);

                    if (progressListener != null) {
                        progressListener.complete();
                    }

                    return tempZipFile;
                } catch (Exception e) {
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Could not complete the Download Process", e));
                    }
                    throw new ProcessException("Could not complete the Download Process", e);
                }
            }
        } else {
            cause = new WPSException("Could not find store for layer " + layerName);
            if (progressListener != null) {
                progressListener.exceptionOccurred(new WPSException(
                        "Could not find store for layer " + layerName));
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
     * @param resourceInfo the resource info
     * @param coverage the coverage
     * @param gc the gc
     * @param roi the roi
     * @param roiCRS the roi crs
     * @param targetCRS the target crs
     * @param cropToGeometry the crop to geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void writeRasterOutput(String outputFormat, final ProgressListener progressListener,
            String extension, OutputStream os, ResourceInfo resourceInfo, CoverageInfo coverage,
            GridCoverage2D gc, Geometry roi, CoordinateReferenceSystem roiCRS,
            CoordinateReferenceSystem targetCRS, boolean cropToGeometry) throws IOException {
        Throwable cause = null;
        GridCoverage2D finalCoverage = null;
        try {
            finalCoverage = getFinalCoverage(resourceInfo, coverage, gc, roi, roiCRS, targetCRS,
                    cropToGeometry, progressListener);
            if (extension.toLowerCase().contains("tif")) {

                GeoTiffWriter gtiffWriter = new GeoTiffWriter(os);

                try {
                    final ParameterValue<GeoToolsWriteParams> gtWparam = AbstractGridFormat.GEOTOOLS_WRITE_PARAMS
                            .createValue();
                    GeoTiffWriteParams param = new GeoTiffWriteParams();
                    gtWparam.setValue(param);
                    GeneralParameterValue[] params = new GeneralParameterValue[] { gtWparam };

                    gtiffWriter.write(finalCoverage, params);
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
                        writer.write(finalCoverage.getRenderedImage());
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
            if (gc != null)
                gc.dispose(true);
            if (finalCoverage != null)
                finalCoverage.dispose(true);
        }
    }

    /**
     * Handle vectorial layer download.
     * 
     * @param resourceInfo the resource info
     * @param store the store info
     * @param filter the filter
     * @param email the email
     * @param mimeType the output format
     * @param targetCRS the target crs
     * @param roiCRS the roi crs
     * @param roi the roi
     * @param cropToGeometry the crop to geometry
     * @param progressListener the progress listener
     * @return the file
     */
    private File handleVector(
            ResourceInfo resourceInfo, 
            DataStoreInfo store,
            Filter filter, 
            String email, 
            String mimeType, 
            CoordinateReferenceSystem targetCRS,
            CoordinateReferenceSystem roiCRS, 
            Geometry roi, 
            Boolean clip,
            final ProgressListener progressListener) throws Exception{
        
        //
        // Initial checks
        //
        if(roi!=null){
            DownloadUtilities.checkPolygonROI(roi);
            if(roiCRS==null){
                throw new IllegalArgumentException("Provided an ROI without a CRS!");
            }
        }


        // prepare native CRS
        CoordinateReferenceSystem nativeCRS=resourceInfo.getNativeCRS();
        if(nativeCRS==null){
            nativeCRS=resourceInfo.getCRS();
        }
        if(nativeCRS==null){
            throw new NullPointerException("Unable to find a valid CRS for the requested feature type");
        }
        
        
        //
        // STEP 0 - Push ROI back to native CRS (if ROI is provided)
        //
        Geometry roiInNativeCRS=roi;
        if(roi!=null){
            MathTransform targetTX = null;
            if (!CRS.equalsIgnoreMetadata(nativeCRS, roiCRS)) {
                // we MIGHT have to reproject
                targetTX = CRS.findMathTransform(roiCRS, nativeCRS) ;      
                // reproject
                if(!targetTX.isIdentity()){
                    roiInNativeCRS=JTS.transform(roi, targetTX);

                    // checks
                    if (roiInNativeCRS==null) {
                        throw new IllegalStateException("The Region of Interest is null after going back to native CRS!");
                    }                     
                    DownloadUtilities.checkPolygonROI(roiInNativeCRS);
                }                     
            }
            
        }
        
        //
        // STEP 1 - Read and Filter
        //
        
        // access feature source and collection of features
        final SimpleFeatureSource featureSource = DownloadUtilities.getFeatureSource(store, resourceInfo, progressListener);
        
        // basic filter preparation
        Filter ra=Filter.INCLUDE;
        if (filter != null){
            ra = filter;
        }
        // and with the ROI if we have one
        SimpleFeatureCollection originalFeatures;
        if(roiInNativeCRS!=null){
            final String dataGeomName = featureSource.getSchema().getGeometryDescriptor().getLocalName();
            final Intersects intersectionFilter = FeatureUtilities.DEFAULT_FILTER_FACTORY.intersects(
                    FeatureUtilities.DEFAULT_FILTER_FACTORY.property(dataGeomName),
                    FeatureUtilities.DEFAULT_FILTER_FACTORY.literal(roiInNativeCRS));                
            ra=FeatureUtilities.DEFAULT_FILTER_FACTORY.and(
                    ra, 
                    intersectionFilter);
        }
        // read
        originalFeatures = featureSource.getFeatures(ra);
        checkIsEmptyFeatureCollection(originalFeatures);
        
        //
        // STEP 2 - Clip
        //
        SimpleFeatureCollection clippedFeatures;
        if(clip){
            final ClipProcess clipProcess= new ClipProcess();
            clippedFeatures=clipProcess.execute(originalFeatures, roiInNativeCRS);

        }else{
            clippedFeatures=originalFeatures;
        }
        //checks
        checkIsEmptyFeatureCollection(clippedFeatures);
        
        //
        // STEP 3 - Reproject feature collection
        //            
        // do we need to reproject?
        SimpleFeatureCollection reprojectedFeatures;
        if (targetCRS != null&&!CRS.equalsIgnoreMetadata(nativeCRS, targetCRS)) {

                // testing reprojection...
                final MathTransform targetTX = CRS.findMathTransform(nativeCRS, targetCRS) ;
                if(!targetTX.isIdentity()){
                    // avoid doing the transform if this is the identity
                        reprojectedFeatures=new ReprojectingFeatureCollection(clippedFeatures, targetCRS);
                    } else {
                        reprojectedFeatures=clippedFeatures;
                    }
            } else {
                reprojectedFeatures=clippedFeatures;
            }
            checkIsEmptyFeatureCollection(reprojectedFeatures);

   

            //
        // STEP  - Write down respecting limits in bytes
        //
        // writing the output, making sure it is a zip
        File output=writeVectorOutput( progressListener, reprojectedFeatures,resourceInfo.getName(),mimeType);

        // Completed!
        if (progressListener != null) {
            progressListener.complete();
        }
        
        // email
        sendMail(email, progressListener, output, false);

        return output;
    }

    /**
     * @param clippedFeatures
     * @throws IllegalStateException
     */
    private void checkIsEmptyFeatureCollection(SimpleFeatureCollection clippedFeatures)
            throws IllegalStateException {
        if (clippedFeatures == null || clippedFeatures.isEmpty()) {
            throw new IllegalStateException("Got an empty feature collection.");
        }
    }

    /**
     * Write vector output with the provided PPIO. It returns the {@link File} it writes to.
     * 
     * @param progressListener
     * @param features
     * @param name
     * @param mimeType 
     * @return
     * @throws Exception 
     */
    private File writeVectorOutput(
            final ProgressListener progressListener,
            final SimpleFeatureCollection features, 
            final String name, 
            final String mimeType) throws Exception {


        // Search a proper PPIO
        ProcessParameterIO ppio_ = DownloadUtilities.find(
                new Parameter<SimpleFeatureCollection>("fakeParam", SimpleFeatureCollection.class), 
                null, 
                mimeType,
                false);
        if (ppio_ == null ) {
            throw new ProcessException("Don't know how to encode in mime type "
                    + mimeType);
        } else if (!(ppio_ instanceof ComplexPPIO)) {
            throw new ProcessException("Invalid PPIO found "
                    + ppio_.getIdentifer());
        }     
        
        // limits
        long limit = Long.MAX_VALUE;
        if( estimator.getHardOutputLimit() > 0 ){
            limit=estimator.getHardOutputLimit() * 1024 ;
        } else if(estimator.getWriteLimits() > 0){
            limit=estimator.getWriteLimits() * 1024;
        }
        
        //
        // Get fileName
        //
        String extension="";
        if(ppio_ instanceof ComplexPPIO){
            extension="."+((ComplexPPIO)ppio_).getFileExtension();
        } 
        
        // create output file
        final File output = File.createTempFile(
                name, 
                extension,
                WPSClusterStorageCleaner.getWpsOutputStorage());        


        // write checking limits
        OutputStream os=null;
        try{
            
            // create OutputStream that checks limits
            os = new LimitedFileOutputStream(new FileOutputStream(output), limit) {

                @Override
                protected void raiseError(long pSizeMax, long pCount) throws IOException {
                    throw new IOException("Download Exceeded the maximum HARD allowed size!");
                }

            };
            
            // write with PPIO
            if(ppio_ instanceof ComplexPPIO){
                ((ComplexPPIO)ppio_).encode(features, os);
            } 
            
        } finally {
            if(os!=null){
                IOUtils.closeQuietly(os);
            }
        }

        // do we need to zip the output??
        if (!FilenameUtils.getExtension(output.getName()).equalsIgnoreCase("zip")) { // TODO this is not robust!
            final ZipArchive zipPPIO = new ZipArchivePPIO.ZipArchive(geoServer, null); // TODO instantiate only once
            final File newOutput = new File(
                    FilenameUtils.getFullPath(output.getAbsolutePath()),
                    FilenameUtils.getBaseName(output.getName()) + ".zip");
            
            FileOutputStream os1 = null;
            try{
                os1=new FileOutputStream(newOutput);
                zipPPIO.encode(output, os1);
                return newOutput; // return
            }finally{
                if(os!=null){
                    IOUtils.closeQuietly(os1);
                }
            }
        } else {

            // return
            return output;
        }
        
    }

    /**
     * Send mail.
     * 
     * @param email the email
     * @param progressListener the progress listener
     * @param output the output
     * @param started the started
     */
    protected void sendMail(String email, final ProgressListener progressListener, File output,
            boolean started) {
        if (sendMail!=null&&email != null && progressListener instanceof ClusterProcessListener) {
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
                LOGGER.log(Level.WARNING,"Could not send the notification email : "
                        + e.getLocalizedMessage(),e);
            }
        }
    }
}