/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.util.logging.Logger;

import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.WPSException;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;

@DescribeProcess(title = "Enterprise Download Process", description = "Downloads Layer Stream and provides a ZIP.")
public class DownloadEstimatorProcess extends AbstractDownloadProcess {

    protected static final Logger LOGGER = Logging.getLogger(DownloadEstimatorProcess.class);

    private static final long DEFAULT_MAX_FEATURES = 1000000;

    private long maxFeatures;

    private long readLimits;

    private long writeLimits;

    public DownloadEstimatorProcess(GeoServer geoServer) {
        super(geoServer);
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
                final DataStoreInfo dataStore = (DataStoreInfo) storeInfo;

                Filter ra = null;
                try {
                    // === filter or expression
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

                    setFeatureSource(getFeatureSource(dataStore, progressListener));

                } catch (Exception e) {
                    if (progressListener != null) {
                        progressListener.exceptionOccurred(new ProcessException(
                                "Error while checking Feature Source", e));
                    }
                    throw new ProcessException("Error while checking Feature Source", e);
                }

                try {
                    int count = getFeatureSource().getCount(new Query("counter", ra));

                    if (count < 0) {
                        SimpleFeatureCollection features = getFeatureSource().getFeatures(ra);
                        count = features.size();
                    }

                    if (count > maxFeatures) {
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
                        getCoverage(coverage, roi, targetCRS, progressListener);

                        /**
                         * Checking that the coverage described by the specified geometry and sample model does not exceeds the read limits
                         */
                        // compute the coverage memory usage and compare with limit
                        long actual = getCoverageSize(getGc().getGridGeometry().getGridRange2D(), getGc()
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
                        actual = getCoverageSize(getFinalCoverage().getGridGeometry().getGridRange2D(),
                                getFinalCoverage().getRenderedImage().getSampleModel());
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

                    } catch (Exception e) {
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

}