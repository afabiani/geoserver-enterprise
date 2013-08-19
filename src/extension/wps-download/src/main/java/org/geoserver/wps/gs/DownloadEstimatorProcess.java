/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.util.logging.Logger;

import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.WPSException;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;

/**
 * The Class DownloadEstimatorProcess.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@DescribeProcess(title = "Enterprise Download Process", description = "Downloads Layer Stream and provides a ZIP.")
public class DownloadEstimatorProcess extends AbstractDownloadProcess {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = Logging.getLogger(DownloadEstimatorProcess.class);

    /** The Constant DEFAULT_MAX_FEATURES. */
    public static final long DEFAULT_MAX_FEATURES = 1000000;

    /** The max features. */
    private long maxFeatures;

    /** The read limits. */
    private long readLimits;

    /** The write limits. */
    private long writeLimits;

    /** The hard output limit. */
    private long hardOutputLimit;

    /**
     * Instantiates a new download estimator process.
     * 
     * @param geoServer the geo server
     */
    public DownloadEstimatorProcess(GeoServer geoServer) {
        super(geoServer);
        this.maxFeatures = DEFAULT_MAX_FEATURES;
        this.readLimits = 0;
        this.writeLimits = 0;
        this.hardOutputLimit = 0;
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
     * @return the boolean
     * @throws ProcessException the process exception
     */
    @DescribeResult(name = "result", description = "Download Limits are respected or not!")
    public Boolean execute(
            @DescribeParameter(name = "layerName", min = 1, description = "Original layer to download") String layerName,
            @DescribeParameter(name = "filter", min = 0, description = "Optional Vectorial Filter") Filter filter,
            @DescribeParameter(name = "email", min = 0, description = "Optional Email Address for notification") String email,
            @DescribeParameter(name = "outputFormat", min = 1, description = "Output Format") String outputFormat,
            @DescribeParameter(name = "targetCRS", min = 0, description = "Target CRS") CoordinateReferenceSystem targetCRS,
            @DescribeParameter(name = "RoiCRS", min = 0, description = "Region Of Interest CRS") CoordinateReferenceSystem roiCRS,
            @DescribeParameter(name = "ROI", min = 0, description = "Region Of Interest") Geometry roi,
            @DescribeParameter(name = "cropToROI", min = 0, description = "Crop to ROI") Boolean cropToGeometry,
            ProgressListener progressListener) throws ProcessException {

        Throwable cause = null;

        if (layerName != null) {
            LayerInfo layerInfo = null;
            ResourceInfo resourceInfo = null;
            StoreInfo storeInfo = null;

            // cheking for the rsources on the GeoServer catalog
            layerInfo = catalog.getLayerByName(layerName);
            resourceInfo = layerInfo.getResource();
            storeInfo = resourceInfo.getStore();

            if (storeInfo == null) {
                // could not find any data store associated to the specified layer ... abruptly interrupt the process
                LOGGER.severe("Unable to locate the resource " + layerName);
                cause = new IllegalArgumentException("Unable to locate feature:"
                        + layerName);
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new ProcessException(
                            "Could not complete the Download Process", cause));
                }
                throw new ProcessException("Could not complete the Download Process", cause);
            }

            // ////
            // 1. DataStore -> look for vectorial data download
            // 2. CoverageStore -> look for raster data download
            // ////
            if (storeInfo instanceof DataStoreInfo) {
                final DataStoreInfo dataStore = (DataStoreInfo) storeInfo;

                SimpleFeatureSource featureSource = null;
                Filter ra = null;
                // The filter is parsed by the GeoServer FilterPPIOs
                try {
                    
                    if (filter == null) ra = Filter.INCLUDE;
                    else ra = filter;
                    
                    /*
                     * OLD Filter Parsing Code

                        // === filter or expression
                        if (filter != null) {
                            try {
                                ra = ECQL.toFilter(filter);
                            } catch (Exception e_cql) {
                                try {
                                    Parser parser = new Parser(
                                            new org.geotools.filter.v1_1.OGCConfiguration());
                                    Reader reader = new StringReader(filter);
                                    // set the input source with the correct encoding
                                    InputSource source = new InputSource(reader);
                                    source.setEncoding(getCharset().name());
                                    ra = (Filter) parser.parse(source);
    
                                } catch (Exception e_ogc) {
                                    cause = new WPSException("Unable to parse input expression", e_ogc);
                                    if (progressListener != null) {
                                        progressListener.exceptionOccurred(new ProcessException(
                                                "Could not complete the Download Process", cause));
                                    }
                                    throw new ProcessException(
                                            "Could not complete the Download Process", cause);
                                }
                            }
                        } else {
                            ra = Filter.INCLUDE;
                        }
                        
                     */

                    // get the feature source given the datastore
                    featureSource = getFeatureSource(dataStore, resourceInfo, progressListener);

                } catch (Exception e) {
                    LOGGER.severe("Could not parse the feature source for " + layerName);
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Error while checking Feature Source", e));
                    }
                    throw new ProcessException("Error while checking Feature Source", e);
                }
                
                // count the features and compare to the "maxfeatures" limit
                long count = maxFeatures;
                try {
                    // try first using the storage count capabilities using the getCount(...) method
                    count = featureSource.getCount(new Query("counter", ra));

                    if (count < 0) {
                        // a value minor than "0" means that the store does not provide any counting feature ... lets proceed using the iterator
                        SimpleFeatureCollection features = featureSource.getFeatures(ra);
                        count = features.size();
                    }

                } catch (Exception e) {
                    LOGGER.severe("Error while checking Feature Download Limits");
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Error while checking Feature Download Limits", e));
                    }
                    throw new ProcessException("Error while checking Feature Download Limits", e);
                }

                // finally checking the number of features accordingly to the "maxfeatures" limit
                if (count > maxFeatures) {
                    LOGGER.severe("MaxFeatures limit exceeded.");
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Features Download Limits: Max allowed features exceeded."));
                    }
                    throw new ProcessException("Max allowed of " + this.maxFeatures
                            + " features exceeded.");
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
                        GridCoverage2D gc = null;
                        try {
                            gc = getCoverage(resourceInfo, coverage, roi, roiCRS,
                                    targetCRS, cropToGeometry, progressListener);
                        } catch (Exception e) {
                            LOGGER.severe("Could not complete the Download Process");
                            cause = e;
                            if (progressListener != null) {
                                progressListener.exceptionOccurred(new ProcessException(
                                        "Could not complete the Download Process", cause));
                            }
                            throw new ProcessException("Could not complete the Download Process", cause);
                        }

                        /**
                         * Checking that the coverage described by the specified geometry and sample model does not exceeds the read limits
                         */
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
                        // compute the coverage memory usage and compare with limit
                        GridCoverage2D finalCoverage = getFinalCoverage(resourceInfo, coverage, gc,
                                roi, roiCRS, targetCRS, cropToGeometry, progressListener);
                        actual = getCoverageSize(finalCoverage.getGridGeometry().getGridRange2D(),
                                finalCoverage.getRenderedImage().getSampleModel());
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

                    return (true);
                }
            } else {
                LOGGER.severe("Could not find store for layer " + layerName);
                cause = new WPSException("Could not find store for layer " + layerName);
                if (progressListener != null) {
                    progressListener.exceptionOccurred(new WPSException(
                            "Could not find store for layer " + layerName));
                }
            }
        } else {
            LOGGER.severe("Could not find layer " + layerName);
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
     * Sets the max features.
     * 
     * @param maxFeatures the maxFeatures to set
     */
    public void setMaxFeatures(long maxFeatures) {
        this.maxFeatures = maxFeatures;
    }

    /**
     * Gets the max features.
     * 
     * @return the maxFeatures
     */
    public long getMaxFeatures() {
        return maxFeatures;
    }

    /**
     * Sets the read limits.
     * 
     * @param readLimits the readLimits to set
     */
    public void setReadLimits(long readLimits) {
        this.readLimits = readLimits;
    }

    /**
     * Gets the read limits.
     * 
     * @return the readLimits
     */
    public long getReadLimits() {
        return readLimits;
    }

    /**
     * Sets the write limits.
     * 
     * @param writeLimits the writeLimits to set
     */
    public void setWriteLimits(long writeLimits) {
        this.writeLimits = writeLimits;
    }

    /**
     * Gets the write limits.
     * 
     * @return the writeLimits
     */
    public long getWriteLimits() {
        return writeLimits;
    }

    /**
     * Sets the hard output limit.
     * 
     * @param hardOutputLimit the hardOutputLimit to set
     */
    public void setHardOutputLimit(long hardOutputLimit) {
        this.hardOutputLimit = hardOutputLimit;
    }

    /**
     * Gets the hard output limit.
     * 
     * @return the hardOutputLimit
     */
    public long getHardOutputLimit() {
        return hardOutputLimit;
    }
}