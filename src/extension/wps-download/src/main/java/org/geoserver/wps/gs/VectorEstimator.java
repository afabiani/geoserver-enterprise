/* Copyright (c) 2012 GeoSolutions http://www.geo-solutions.it. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.geoserver.catalog.FeatureTypeInfo;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.resources.coverage.FeatureUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.filter.spatial.Intersects;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;

class VectorEstimator {

    /**
     * @param estimator
     * @param geoServer
     */
    public VectorEstimator(DownloadEstimatorProcess estimator) {
        this.estimator = estimator;
    }

    static final Logger LOGGER = Logging.getLogger(VectorEstimator.class);

    /** The estimator. */
    private DownloadEstimatorProcess estimator;

    public boolean execute(FeatureTypeInfo resourceInfo, Geometry roi, boolean clip, Filter filter,
            CoordinateReferenceSystem targetCRS, final ProgressListener progressListener)
            throws Exception {
        //
        // Do we need to do anything?
        //
        if (estimator.getMaxFeatures() <= 0) {
            return true;
        }

        // prepare native CRS
        CoordinateReferenceSystem nativeCRS = DownloadUtilities.getNativeCRS(resourceInfo);
        if(LOGGER.isLoggable(Level.FINE)){
            LOGGER.fine("Native CRS is "+nativeCRS.toWKT());
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
        // STEP 1 - Create the Filter
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
        int count = featureSource.getCount(new Query("counter", ra));
        if (count < 0) {
            // a value minor than "0" means that the store does not provide any counting feature ... lets proceed using the iterator
            SimpleFeatureCollection features = featureSource.getFeatures(ra);
            count = features.size();
        }
        // finally checking the number of features accordingly to the "maxfeatures" limit
        final long maxFeatures = estimator.getMaxFeatures();
        if (maxFeatures > 0 && count > maxFeatures) {
            LOGGER.severe("MaxFeatures limit exceeded. " + count + " > " + maxFeatures);
            return false;
        }

        // limits were not exceeded
        return true;
    }
}