package org.geoserver.wps.gs;

import it.geosolutions.imageio.stream.output.FileImageOutputStreamExtImpl;
import it.geosolutions.io.output.adapter.OutputStreamAdapter;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.stream.ImageOutputStream;

import org.geoserver.catalog.CoverageInfo;
import org.geoserver.config.GeoServerDataDirectory;
import org.geoserver.data.util.CoverageUtils;
import org.geoserver.wps.WPSClusterStorageCleaner;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.processing.Operations;
import org.geotools.data.Parameter;
import org.geotools.factory.GeoTools;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.raster.gs.CropCoverage;
import org.geotools.referencing.CRS;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.parameter.GeneralParameterDescriptor;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;

import com.vividsolutions.jts.geom.Geometry;

class RasterDownload {

    static final Logger LOGGER = Logging.getLogger(RasterDownload.class);

    /** The estimator. */
    private DownloadEstimatorProcess estimator;

    /**
     * @param estimator
     * @param geoServer
     */
    public RasterDownload(DownloadEstimatorProcess estimator) {
        this.estimator = estimator;
    }

    /**
     * 
     * @param coverage
     * @param roi
     * @param roiCRS
     * @param targetCRS
     * @return
     */
    public File execute(String mimeType, final ProgressListener progressListener,
            CoverageInfo coverageInfo, Geometry roi, CoordinateReferenceSystem targetCRS,
            boolean clip, Filter filter) throws Exception {

        GridCoverage2D clippedGridCoverage = null, reprojectedGridCoverage = null, originalGridCoverage = null;
        try {

            //
            // look for output extension. Tiff/tif/geotiff will be all threated as GeoTIFF
            //

            //
            // ---> READ FROM NATIVE RESOLUTION <--
            //

            // prepare native CRS
            CoordinateReferenceSystem nativeCRS = coverageInfo.getNativeCRS();
            if (nativeCRS == null) {
                nativeCRS = coverageInfo.getCRS();
            }
            if (nativeCRS == null) {
                throw new NullPointerException(
                        "Unable to find a valid CRS for the requested feature type");
            }
            //
            // STEP 1 - Reproject if needed
            //
            boolean reproject = false;
            MathTransform reprojectionTrasform = null;
            if (targetCRS != null && !CRS.equalsIgnoreMetadata(nativeCRS, targetCRS)) {

                // testing reprojection...
                reprojectionTrasform = CRS.findMathTransform(nativeCRS, targetCRS,true);
                if (!reprojectionTrasform.isIdentity()) {
                    // avoid doing the transform if this is the identity
                    reproject = true;
                }
            }

            //
            // STEP 0 - Push ROI back to native CRS (if ROI is provided)
            //
            CoordinateReferenceSystem roiCRS = null;
            Geometry roiInNativeCRS = roi;
            if (roi != null) {
                roiCRS = (CoordinateReferenceSystem) roi.getUserData();
                MathTransform targetTX = null;
                if (!CRS.equalsIgnoreMetadata(nativeCRS, roiCRS)) {
                    // we MIGHT have to reproject
                    targetTX = CRS.findMathTransform(roiCRS, nativeCRS,true);
                    // reproject
                    if (!targetTX.isIdentity()) {
                        roiInNativeCRS = JTS.transform(roi, targetTX);

                        // checks
                        if (roiInNativeCRS == null) {
                            throw new IllegalStateException(
                                    "The Region of Interest is null after going back to native CRS!");
                        }
                        DownloadUtilities.checkPolygonROI(roiInNativeCRS);
                        roiInNativeCRS.setUserData(nativeCRS); // set new CRS
                    }
                }

            }
            // get a reader for this CoverageInfo
            final AbstractGridCoverage2DReader reader = (AbstractGridCoverage2DReader) coverageInfo
                    .getGridCoverageReader(null, null);
            final ParameterValueGroup readParametersDescriptor = reader.getFormat()
                    .getReadParameters();
            final List<GeneralParameterDescriptor> parameterDescriptors = readParametersDescriptor
                    .getDescriptor().descriptors();
            // get the configured metadata for this coverage without
            GeneralParameterValue[] readParameters = CoverageUtils.getParameters(
                    readParametersDescriptor, coverageInfo.getParameters(), false);

            // merge support for filter
            if (filter != null) {
                readParameters = CoverageUtils.mergeParameter(parameterDescriptors, readParameters,
                        filter, "FILTER", "Filter");
            }
            // read GridGeometry preparation
            if (roi != null) {
                final ReferencedEnvelope roiEnvelope = new ReferencedEnvelope(
                        roiInNativeCRS.getEnvelopeInternal(), nativeCRS);
                GridGeometry2D gg2D = new GridGeometry2D(PixelInCell.CELL_CENTER,
                        reader.getOriginalGridToWorld(PixelInCell.CELL_CENTER), roiEnvelope,
                        GeoTools.getDefaultHints());
                if (reproject) {
                    // enlarge GridRange by 20 px in each direction
                    Rectangle gr2D = gg2D.getGridRange2D();
                    gr2D.grow(20, 20);
                    // new GG2D
                    gg2D = new GridGeometry2D(new GridEnvelope2D(gr2D), gg2D.getGridToCRS(),
                            gg2D.getCoordinateReferenceSystem());
                }
                // TODO make sure the GridRange is not empty, depending on the resolution it might happen
                readParameters = CoverageUtils.mergeParameter(parameterDescriptors, readParameters,
                        gg2D, AbstractGridFormat.READ_GRIDGEOMETRY2D.getName().getCode());
            }
            // make sure we work in streaming fashion
            readParameters = CoverageUtils.mergeParameter(parameterDescriptors, readParameters,
                    Boolean.TRUE, AbstractGridFormat.USE_JAI_IMAGEREAD.getName().getCode());

            // --> READ
            originalGridCoverage = (GridCoverage2D) reader.read(readParameters);
            //

            //
            // STEP 1 - Reproject if needed
            //
            if (reproject) {
                // avoid doing the transform if this is the identity
                reprojectedGridCoverage = (GridCoverage2D) Operations.DEFAULT.resample(
                        originalGridCoverage, targetCRS);

            } else {
                reprojectedGridCoverage = originalGridCoverage;
            }

            //
            // STEP 2 - Clip if needed
            //
            // we need to push the ROI to the final CRS to crop or CLIP
            if (roi != null) {
                Geometry roiInTargetCRS = roi;

                // do we need to reproject the roi to target CRS for clipping/cropping
                if (targetCRS != null) {
                    MathTransform targetTX = null;
                    if (!CRS.equalsIgnoreMetadata(roiCRS, targetCRS)) {
                        // we MIGHT have to reproject
                        targetTX = CRS.findMathTransform(roiCRS, targetCRS,true);
                        // reproject
                        if (!targetTX.isIdentity()) {
                            roiInTargetCRS = JTS.transform(roi, targetTX);

                            // checks
                            if (roiInTargetCRS == null) {
                                throw new IllegalStateException(
                                        "The Region of Interest is null after going back to native CRS!");
                            }
                            DownloadUtilities.checkPolygonROI(roiInTargetCRS);
                            roiInTargetCRS.setUserData(targetCRS);
                        }
                    }
                } else {
                    roiInTargetCRS = roiInNativeCRS;
                }

                // Crop or Clip
                final CropCoverage cropCoverage = new CropCoverage(); // TODO avoid creation
                if (clip) {
                    // clipping means carefully following the ROI shape
                    clippedGridCoverage = cropCoverage.execute(reprojectedGridCoverage,
                            roiInTargetCRS, progressListener);
                } else {
                    // use envelope of the ROI to simply crop and not clip the raster. This is important since when
                    // reprojecting we might read a bit more than needed!
                    final Geometry polygon = roiInTargetCRS.getEnvelope();
                    polygon.setUserData(targetCRS);
                    clippedGridCoverage = cropCoverage.execute(reprojectedGridCoverage,
                            roiInTargetCRS, progressListener);
                }
            } else {
                // do nothing
                clippedGridCoverage = reprojectedGridCoverage;
            }

            //
            // STEP 3 - Writing
            //
            final File output = writeRaster(mimeType, coverageInfo, clippedGridCoverage);

            // return
            return output;
        } finally {
            if (originalGridCoverage != null)
                originalGridCoverage.dispose(true);
            if (reprojectedGridCoverage != null)
                reprojectedGridCoverage.dispose(true);
            if (clippedGridCoverage != null)
                clippedGridCoverage.dispose(true);
        }
    }

    /**
     * 
     * @param mimeType
     * @param coverageInfo
     * @param gridCoverage
     * @return
     * @throws Exception
     */
    private File writeRaster(String mimeType, CoverageInfo coverageInfo, GridCoverage2D gridCoverage)
            throws Exception {

        // limits
        long limit = DownloadEstimatorProcess.NO_LIMIT;
        if (estimator.getHardOutputLimit() > 0) {
            limit = estimator.getHardOutputLimit();
        }

        // TODO refactor to use MIME-TYPE
        // Search a proper PPIO
        ProcessParameterIO ppio_ = DownloadUtilities.find(new Parameter<GridCoverage2D>(
                "fakeParam", GridCoverage2D.class), null, mimeType, false);
        if (ppio_ == null) {
            throw new ProcessException("Don't know how to encode in mime type " + mimeType);
        } else if (!(ppio_ instanceof ComplexPPIO)) {
            throw new ProcessException("Invalid PPIO found " + ppio_.getIdentifer());
        }
        final ComplexPPIO complexPPIO = (ComplexPPIO) ppio_;
        String extension = complexPPIO.getFileExtension();

        // writing the output to a temporary folder
        final File output = File.createTempFile(coverageInfo.getName(), "." + extension,
                GeoserverDataDirectory.findCreateConfigDir("temp"));
        output.deleteOnExit();

        // the limit ouutput stream will throw an exception if the process is trying to writr more than the max allowed bytes
        final FileImageOutputStreamExtImpl fileImageOutputStreamExtImpl = new FileImageOutputStreamExtImpl(
                output);
        final ImageOutputStream os;
        if (limit > DownloadEstimatorProcess.NO_LIMIT) {
            os = new LimitedImageOutputStream(fileImageOutputStreamExtImpl, limit) {

                @Override
                protected void raiseError(long pSizeMax, long pCount) throws IOException {
                    IOException e = new IOException(
                            "Download Exceeded the maximum HARD allowed size!");
                    throw e;
                }
            };
        } else {
            os = fileImageOutputStreamExtImpl;
        }
        // write
        try {
            complexPPIO.encode(gridCoverage, new OutputStreamAdapter(os));
        } finally {
            try {
                if (os != null) {
                    os.close();
                }
            } catch (Exception e) {
                LOGGER.log(Level.FINE, e.getLocalizedMessage(), e);
            }
        }
        return output;
    }
}