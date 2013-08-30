package org.geoserver.wps.gs;

import java.awt.geom.Rectangle2D;
import java.util.logging.Logger;

import org.geoserver.catalog.CoverageInfo;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.resources.coverage.FeatureUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

class RasterEstimator {

    static final Logger LOGGER = Logging.getLogger(RasterEstimator.class);
    
    /** The estimator. */
    private DownloadEstimatorProcess estimator;
    

    
    /**
     * @param estimator
     * @param geoServer
     */
    public RasterEstimator(DownloadEstimatorProcess estimator ) {
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
    public boolean execute(
            final ProgressListener progressListener,
            CoverageInfo coverageInfo,
            Geometry roi, 
            CoordinateReferenceSystem targetCRS, 
            boolean clip,
            Filter filter) throws Exception {
    
        // 
        // Do we need to do anything?
        // 
        if(estimator.getReadLimits()<=0){
            return true;
        }
    
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
        // STEP 0 - Push ROI back to native CRS (if ROI is provided)
        //
        CoordinateReferenceSystem roiCRS = null;
        Geometry roiInNativeCRS = roi;
        if (roi != null) {
            roiCRS = (CoordinateReferenceSystem) roi.getUserData();
            MathTransform targetTX = null;
            if (!CRS.equalsIgnoreMetadata(nativeCRS, roiCRS)) {
                // we MIGHT have to reproject
                targetTX = CRS.findMathTransform(roiCRS, nativeCRS);
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
        final AbstractGridCoverage2DReader reader = (AbstractGridCoverage2DReader) coverageInfo.getGridCoverageReader(null, null);
        

        // read GridGeometry preparation
        final double areaRead;
        if(roi!=null){
            Geometry roiInNativeCRS_ = roiInNativeCRS.intersection(FeatureUtilities.getPolygon(reader.getOriginalEnvelope(),new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING))));
            if(roiInNativeCRS_.isEmpty()){
                return true; // EMPTY Intersection
            }
            final AffineTransform2D w2G = (AffineTransform2D) reader.getOriginalGridToWorld(PixelInCell.CELL_CORNER).inverse();
            Geometry rasterGeometry = JTS.transform(roiInNativeCRS_, w2G);
            areaRead=rasterGeometry.getEnvelope().getArea();
            // TODO improve precision taking into account tiling on raster geometry
            
        } else {
            // No ROI, we are trying to read the entire coverage
            final Rectangle2D originalGridRange = (GridEnvelope2D) reader.getOriginalGridRange();
            areaRead=originalGridRange.getWidth()*originalGridRange.getHeight();

        }
        // checks on the area we want to download
        final long readLimits = estimator.getReadLimits();
        if(readLimits>0&&areaRead>readLimits){
            return false;
        }     
        return true;

    }
}