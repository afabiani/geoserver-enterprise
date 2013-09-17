package org.geoserver.wps.gs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.wps.ppio.ComplexPPIO;
import org.geoserver.wps.ppio.ProcessParameterIO;
import org.geotools.data.Parameter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.geometry.jts.JTS;
import org.geotools.process.ProcessException;
import org.geotools.process.feature.gs.ClipProcess;
import org.geotools.referencing.CRS;
import org.geotools.resources.coverage.FeatureUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.filter.spatial.Intersects;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;

import com.vividsolutions.jts.geom.Geometry;

class VectorDownload {

    /**
     * @param estimator
     * @param geoServer
     */
    public VectorDownload(DownloadEstimatorProcess estimator) {
        this.estimator = estimator;
    }

    static final Logger LOGGER = Logging.getLogger(VectorDownload.class);

    /** The estimator. */
    private DownloadEstimatorProcess estimator;

    public File execute(FeatureTypeInfo resourceInfo, String mimeType, Geometry roi, boolean clip,
            Filter filter, CoordinateReferenceSystem targetCRS,
            final ProgressListener progressListener) throws Exception {

        // prepare native CRS
        CoordinateReferenceSystem nativeCRS = resourceInfo.getNativeCRS();
        if (nativeCRS == null) {
            nativeCRS = resourceInfo.getCRS();
        }
        if (nativeCRS == null) {
            throw new NullPointerException(
                    "Unable to find a valid CRS for the requested feature type");
        }

        //
        // STEP 0 - Push ROI back to native CRS (if ROI is provided)
        //
        Geometry roiInNativeCRS = roi;
        if (roi != null) {
            CoordinateReferenceSystem roiCRS = (CoordinateReferenceSystem) roi.getUserData();
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
                    roiInNativeCRS.setUserData(nativeCRS);
                }
            }

        }

        //
        // STEP 1 - Read and Filter
        //

        // access feature source and collection of features
        final SimpleFeatureSource featureSource = (SimpleFeatureSource) resourceInfo
                .getFeatureSource(null, null); // TODO hints!!!

        // basic filter preparation
        Filter ra = Filter.INCLUDE;
        if (filter != null) {
            ra = filter;
        }
        // and with the ROI if we have one
        SimpleFeatureCollection originalFeatures;
        if (roiInNativeCRS != null) {
            final String dataGeomName = featureSource.getSchema().getGeometryDescriptor()
                    .getLocalName();
            final Intersects intersectionFilter = FeatureUtilities.DEFAULT_FILTER_FACTORY
                    .intersects(FeatureUtilities.DEFAULT_FILTER_FACTORY.property(dataGeomName),
                            FeatureUtilities.DEFAULT_FILTER_FACTORY.literal(roiInNativeCRS));
            ra = FeatureUtilities.DEFAULT_FILTER_FACTORY.and(ra, intersectionFilter);
        }

        // simpplify filter
        ra = (Filter) ra.accept(new SimplifyingFilterVisitor(), null);
        // read
        originalFeatures = featureSource.getFeatures(ra);
        DownloadUtilities.checkIsEmptyFeatureCollection(originalFeatures);

        //
        // STEP 2 - Clip
        //
        SimpleFeatureCollection clippedFeatures;
        if (clip && roi != null) {
            final ClipProcess clipProcess = new ClipProcess();// TODO avoid unnecessary creation
            clippedFeatures = clipProcess.execute(originalFeatures, roiInNativeCRS);

            // checks
            DownloadUtilities.checkIsEmptyFeatureCollection(clippedFeatures);
        } else {
            clippedFeatures = originalFeatures;
        }

        //
        // STEP 3 - Reproject feature collection
        //
        // do we need to reproject?
        SimpleFeatureCollection reprojectedFeatures;
        if (targetCRS != null && !CRS.equalsIgnoreMetadata(nativeCRS, targetCRS)) {

            // testing reprojection...
            final MathTransform targetTX = CRS.findMathTransform(nativeCRS, targetCRS,true);
            if (!targetTX.isIdentity()) {
                // avoid doing the transform if this is the identity
                reprojectedFeatures = new ReprojectingFeatureCollection(clippedFeatures, targetCRS);
            } else {
                reprojectedFeatures = clippedFeatures;
                DownloadUtilities.checkIsEmptyFeatureCollection(reprojectedFeatures);
            }
        } else {
            reprojectedFeatures = clippedFeatures;
        }

        //
        // STEP 4 - Write down respecting limits in bytes
        //
        // writing the output, making sure it is a zip
        return writeVectorOutput(progressListener, reprojectedFeatures, resourceInfo.getName(),
                mimeType);

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
    private File writeVectorOutput(final ProgressListener progressListener,
            final SimpleFeatureCollection features, final String name, final String mimeType)
            throws Exception {

        // Search a proper PPIO
        ProcessParameterIO ppio_ = DownloadUtilities.find(new Parameter<SimpleFeatureCollection>(
                "fakeParam", SimpleFeatureCollection.class), null, mimeType, false);
        if (ppio_ == null) {
            throw new ProcessException("Don't know how to encode in mime type " + mimeType);
        } else if (!(ppio_ instanceof ComplexPPIO)) {
            throw new ProcessException("Invalid PPIO found " + ppio_.getIdentifer());
        }

        // limits
        long limit = DownloadEstimatorProcess.NO_LIMIT;
        if (estimator.getHardOutputLimit() > 0) {
            limit = estimator.getHardOutputLimit();
        }

        //
        // Get fileName
        //
        String extension = "";
        if (ppio_ instanceof ComplexPPIO) {
            extension = "." + ((ComplexPPIO) ppio_).getFileExtension();
        }

        // create output file
        final File output = File.createTempFile(name, extension,
                GeoserverDataDirectory.findCreateConfigDir("temp"));

        // write checking limits
        OutputStream os = null;
        try {

            // create OutputStream that checks limits
            final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
                    new FileOutputStream(output));
            if (limit > DownloadEstimatorProcess.NO_LIMIT) {
                os = new LimitedOutputStream(bufferedOutputStream, limit) {

                    @Override
                    protected void raiseError(long pSizeMax, long pCount) throws IOException {
                        throw new IOException("Download Exceeded the maximum HARD allowed size!");
                    }

                };

            } else {
                os = bufferedOutputStream;
            }

            // write with PPIO
            if (ppio_ instanceof ComplexPPIO) {
                ((ComplexPPIO) ppio_).encode(features, os);
            }

        } finally {
            if (os != null) {
                IOUtils.closeQuietly(os);
            }
        }

        // return
        return output;

    }
}