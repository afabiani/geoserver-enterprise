/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.data.test.MockData;
import org.geoserver.data.util.IOUtils;
import org.geoserver.test.GeoServerTestSupport;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.geoserver.wps.ppio.WFSPPIO;
import org.geoserver.wps.ppio.ZipArchivePPIO;
import org.geoserver.wps.ppio.ZipArchivePPIO.ZipArchive;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.DataUtilities;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.WKTReader2;
import org.geotools.process.ProcessException;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.util.InternationalString;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;

/**
 * The Class DownloadlProcessTest.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class DownloadlProcessTest extends GeoServerTestSupport {

    /**
     * Populate data directory.
     * 
     * @param dataDirectory the data directory
     * @throws Exception the exception
     */
    @Override
    protected void populateDataDirectory(MockData dataDirectory) throws Exception {
        super.populateDataDirectory(dataDirectory);

        dataDirectory.addWcs10Coverages();

        new File(dataDirectory.getDataDirectoryRoot(), "wps-cluster").mkdirs();
        dataDirectory.copyTo(
                DownloadlProcessTest.class.getClassLoader().getResourceAsStream(
                        "wps-cluster/dbConfig.properties"), "wps-cluster/dbConfig.properties");
        dataDirectory.copyTo(
                DownloadlProcessTest.class.getClassLoader().getResourceAsStream(
                        "wps-cluster/downloadProcess.properties"),
                "wps-cluster/downloadProcess.properties");
        dataDirectory.copyTo(
                DownloadlProcessTest.class.getClassLoader().getResourceAsStream(
                        "wps-cluster/mail.properties"), "wps-cluster/mail.properties");
        dataDirectory.copyTo(
                DownloadlProcessTest.class.getClassLoader().getResourceAsStream(
                        "wps-cluster/wpsCluster.properties"), "wps-cluster/wpsCluster.properties");
    }

    /** Test download of vectorial data. */

    final static Polygon roi;
    static {
        try {
            roi = (Polygon) new WKTReader2()
                    .read("POLYGON (( 500116.08576537756 499994.25579707103, 500116.08576537756 500110.1012210889, 500286.2657688021 500110.1012210889, 500286.2657688021 499994.25579707103, 500116.08576537756 499994.25579707103 ))");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test get features as shapefile.
     * 
     * @throws Exception the exception
     */
    public void testGetFeaturesAsShapefile() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POLYGONS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        File shpeZip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "shape-zip", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:32615"), // roiCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(shpeZip);

        SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) decodeShape(new FileInputStream(
                shpeZip));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());
    }

    /**
     * Test get features as shapefile with a different outputCRS from the native one.
     * 
     * @throws Exception the exception
     */

    // DISABLED: The test case always returns
    // Caused by: java.lang.RuntimeException: Unrecognized target type com.vividsolutions.jts.geom.Polygon
    // at org.geotools.process.feature.gs.ClipProcess$ClippingFeatureIterator.clipGeometry(ClipProcess.java:275)
    // at org.geotools.process.feature.gs.ClipProcess$ClippingFeatureIterator.hasNext(ClipProcess.java:195)

    // public void testGetProjectedFeaturesAsShapefile() throws Exception {
    // DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null, null);
    //
    // FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POLYGONS));
    // SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
    // null).getFeatures();
    //
    // File shpeZip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
    // null, // filter
    // null, // mail
    // "shape-zip", // outputFormat
    // CRS.decode("EPSG:4326"), // targetCRS
    // CRS.decode("EPSG:32615"), // roiCRS
    // roi, // roi
    // true, // cropToGeometry
    // new NullProgressListener() // progressListener
    // );
    //
    // assertNotNull(shpeZip);
    //
    // SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) decodeShape(new FileInputStream(
    // shpeZip));
    //
    // assertNotNull(rawTarget);
    //
    // assertEquals(rawSource.size(), rawTarget.size());
    // }

    /**
     * Test filtered clipped features.
     * 
     * @throws Exception the exception
     */
    public void testFilteredClippedFeatures() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON ((0.0008993124415341 0.0006854377923293, 0.0008437876520112 0.0006283489242283, 0.0008566913002806 0.0005341131898971, 0.0009642217025257 0.0005188634237605, 0.0011198475210477 0.000574779232928, 0.0010932581852198 0.0006572843779233, 0.0008993124415341 0.0006854377923293))");

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.BUILDINGS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        File shpeZip = downloadProcess.execute(getLayerId(MockData.BUILDINGS), // layerName
                CQL.toFilter("ADDRESS = '123 Main Street'"), // filter
                null, // mail
                "shape-zip", // outputFormat
                null, // targetCRS
                DefaultGeographicCRS.WGS84, // roiCRS
                roi, // roi
                true, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(shpeZip);

        SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) decodeShape(new FileInputStream(
                shpeZip));

        assertNotNull(rawTarget);

        assertEquals(1, rawTarget.size());

        SimpleFeature srcFeature = rawSource.features().next();
        SimpleFeature trgFeature = rawTarget.features().next();

        assertEquals(srcFeature.getAttribute("ADDRESS"), trgFeature.getAttribute("ADDRESS"));

        Geometry srcGeometry = (Geometry) srcFeature.getDefaultGeometry();
        Geometry trgGeometry = (Geometry) trgFeature.getDefaultGeometry();

        assertTrue("Target geometry clipped and included into the source one",
                srcGeometry.contains(trgGeometry));
    }

    /**
     * Test get features as gml.
     * 
     * @throws Exception the exception
     */
    public void testGetFeaturesAsGML() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POLYGONS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        // GML 2
        File gml2Zip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "application/gml-2.1.2", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:32615"), // roiCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(gml2Zip);

        File[] files = exctractGMLFile(gml2Zip);

        SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) new WFSPPIO.WFS10()
                .decode(new FileInputStream(files[0]));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());

        // GML 3
        File gml3Zip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "application/gml-3.1.1", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:32615"), // roiCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(gml3Zip);

        files = exctractGMLFile(gml2Zip);

        rawTarget = (SimpleFeatureCollection) new WFSPPIO.WFS11().decode(new FileInputStream(
                files[0]));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());
    }

    /**
     * @param gml2Zip
     * @return
     * @throws IOException
     */
    private File[] exctractGMLFile(File gml2Zip) throws IOException {
        IOUtils.decompress(gml2Zip, gml2Zip.getParentFile());

        File[] files = gml2Zip.getParentFile().listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return FilenameUtils.getExtension(name).equalsIgnoreCase("xml");
            }
        });
        return files;
    }

    /**
     * @param jsonZip
     * @return
     * @throws IOException
     */
    private File[] exctractJSONFile(File jsonZip) throws IOException {
        IOUtils.decompress(jsonZip, jsonZip.getParentFile());

        File[] files = jsonZip.getParentFile().listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return FilenameUtils.getExtension(name).equalsIgnoreCase("json");
            }
        });
        return files;
    }

    /**
     * @param gtiffZip
     * @return
     * @throws IOException
     */
    private File[] exctractTIFFFile(final File gtiffZip) throws IOException {
        IOUtils.decompress(gtiffZip, gtiffZip.getParentFile());

        File[] files = gtiffZip.getParentFile().listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return

                (FilenameUtils.getBaseName(gtiffZip.getName()).equals(
                        FilenameUtils.getBaseName(name)) && (FilenameUtils.getExtension(name)
                        .equalsIgnoreCase("tif")
                        || FilenameUtils.getExtension(name).equalsIgnoreCase("tiff") || FilenameUtils
                        .getExtension(name).equalsIgnoreCase("geotiff")));
            }
        });
        return files;
    }

    /**
     * Test get features as geo json.
     * 
     * @throws Exception the exception
     */
    public void testGetFeaturesAsGeoJSON() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POLYGONS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        File jsonZip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "json", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:32615"), // roiCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(jsonZip);

        File[] files = exctractJSONFile(jsonZip);

        SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) new FeatureJSON()
                .readFeatureCollection(new FileInputStream(files[0]));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());
    }

    /**
     * Test download of raster data.
     * 
     * @throws Exception the exception
     */
    public void testDownloadRaster() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        // Envelope env = new Envelope(-125.074006936869,-123.88300771369998, 48.5552612829,49.03872);
        // Polygon roi = JTS.toGeometry(env);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON (( -127.57473954542964 54.06575021619523, -130.8545966116691 52.00807146727025, -129.50812897394974 49.85372324691927, -130.5300633861675 49.20465679591609, -129.25955033314003 48.60392508062591, -128.00975216684665 50.986137055052474, -125.8623089087404 48.63154492960477, -123.984159178178 50.68231871628503, -126.91186316993704 52.15307567440926, -125.3444367403868 53.54787804784162, -127.57473954542964 54.06575021619523 ))");
        roi.setSRID(4326);

        Polygon roiResampled = (Polygon) JTS.transform(
                roi,
                CRS.findMathTransform(CRS.decode("EPSG:4326", true),
                        CRS.decode("EPSG:900913", true)));

        File rasterZip = downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                null, // filter
                null, // mail
                "geotiff", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:4326", true), // roiCRS
                roi, // roi
                true, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(rasterZip);

        File resampledZip = downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                null, // filter
                null, // mail
                "geotiff", // outputFormat
                CRS.decode("EPSG:900913", true), // targetCRS
                CRS.decode("EPSG:900913", true), // roiCRS
                roiResampled, // roi
                true, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(resampledZip);

        GeoTiffReader reader = null;
        GridCoverage2D gc = null, gcResampled = null;
        try {
            reader = new GeoTiffReader(exctractTIFFFile(rasterZip)[0]);
            gc = reader.read(null);

            assertNotNull(gc);

            assertEquals(-130.8545966116691, gc.getEnvelope().getLowerCorner().getOrdinate(0));
            assertEquals(48.60392508062592, gc.getEnvelope().getLowerCorner().getOrdinate(1));
            assertEquals(-123.984159178178, gc.getEnvelope().getUpperCorner().getOrdinate(0));
            assertEquals(54.06575021619523, gc.getEnvelope().getUpperCorner().getOrdinate(1));

        } finally {
            if (gc != null)
                gc.dispose(true);
            if (reader != null)
                reader.dispose();
        }

        try {
            reader = new GeoTiffReader(exctractTIFFFile(resampledZip)[0]);
            gcResampled = reader.read(null);

            assertNotNull(gcResampled);

            assertEquals(-1.4566280988264106E7, gcResampled.getEnvelope().getLowerCorner()
                    .getOrdinate(0));
            assertEquals(6207921.047800108,
                    gcResampled.getEnvelope().getLowerCorner().getOrdinate(1));
            assertEquals(-1.3801853466146953E7, gcResampled.getEnvelope().getUpperCorner()
                    .getOrdinate(0));
            assertEquals(7182130.852574151,
                    gcResampled.getEnvelope().getUpperCorner().getOrdinate(1));

        } finally {
            if (gcResampled != null)
                gcResampled.dispose(true);
            if (reader != null)
                reader.dispose();
        }

    }

    /**
     * PPIO Test.
     * 
     * @throws Exception the exception
     */
    public void testZipPPIO() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        // -130.88669845369998 : -123.88300771369998, 48.5552612829 : 54.1420338629
        Envelope env = new Envelope(-125.074006936869, -123.88300771369998, 48.5552612829, 49.03872);
        Polygon roi = JTS.toGeometry(env);

        File rasterZip = downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                null, // filter
                null, // mail
                "geotiff", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:4326"), // roiCRS
                roi, // roi
                true, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(rasterZip);

        ZipArchive ppio = new ZipArchivePPIO.ZipArchive(getGeoServer(), null);

        File tempZipFile = File.createTempFile("zipppiotemp", ".zip");
        ppio.encode(rasterZip, new FileOutputStream(tempZipFile));

        assertTrue(tempZipFile.length() > 0);

        File tempFile = (File) ppio.decode(new FileInputStream(tempZipFile));

        assertNotNull(tempFile);
    }

    /**
     * Test download estimator for raster data.
     * 
     * @throws Exception the exception
     */
    public void testDownloadEstimatorReadLimitsRaster() throws Exception {

        DownloadEstimatorProcess estimator = new DownloadEstimatorProcess(getGeoServer());
        estimator.setReadLimits(10);

        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, estimator);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON (( -127.57473954542964 54.06575021619523, -130.8545966116691 52.00807146727025, -129.50812897394974 49.85372324691927, -130.5300633861675 49.20465679591609, -129.25955033314003 48.60392508062591, -128.00975216684665 50.986137055052474, -125.8623089087404 48.63154492960477, -123.984159178178 50.68231871628503, -126.91186316993704 52.15307567440926, -125.3444367403868 53.54787804784162, -127.57473954542964 54.06575021619523 ))");
        roi.setSRID(4326);

        try {
            downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                    null, // filter
                    null, // mail
                    "geotiff", // outputFormat
                    null, // targetCRS
                    CRS.decode("EPSG:4326", true), // roiCRS
                    roi, // roi
                    true, // cropToGeometry
                    new NullProgressListener() // progressListener
                    );
        } catch (ProcessException e) {
            assertEquals(
                    "Could not complete the Download Process: This request is trying to read too much data, the limit is 10B but the actual amount of bytes to be read is 11,32MB",
                    e.getMessage() + (e.getCause() != null ? ": " + e.getCause().getMessage() : ""));
            return;
        }

        assertFalse(true);
    }

    /**
     * Test download estimator write limits raster.
     * 
     * @throws Exception the exception
     */
    public void testDownloadEstimatorWriteLimitsRaster() throws Exception {

        DownloadEstimatorProcess estimator = new DownloadEstimatorProcess(getGeoServer());
        estimator.setWriteLimits(10);

        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, estimator);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON (( -127.57473954542964 54.06575021619523, -130.8545966116691 52.00807146727025, -129.50812897394974 49.85372324691927, -130.5300633861675 49.20465679591609, -129.25955033314003 48.60392508062591, -128.00975216684665 50.986137055052474, -125.8623089087404 48.63154492960477, -123.984159178178 50.68231871628503, -126.91186316993704 52.15307567440926, -125.3444367403868 53.54787804784162, -127.57473954542964 54.06575021619523 ))");
        roi.setSRID(4326);

        try {
            downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                    null, // filter
                    null, // mail
                    "geotiff", // outputFormat
                    null, // targetCRS
                    CRS.decode("EPSG:4326", true), // roiCRS
                    roi, // roi
                    true, // cropToGeometry
                    new NullProgressListener() // progressListener
                    );
        } catch (ProcessException e) {
            assertEquals(
                    "Could not complete the Download Process: This request is trying to generate too much data, the limit is 10B but the actual amount of bytes to be written in the output is 10,86MB",
                    e.getMessage() + (e.getCause() != null ? ": " + e.getCause().getMessage() : ""));
            return;
        }

        assertFalse(true);
    }

    /**
     * Test download estimator for vectorial data.
     * 
     * @throws Exception the exception
     */
    public void testDownloadEstimatorMaxFeaturesLimit() throws Exception {

        DownloadEstimatorProcess estimator = new DownloadEstimatorProcess(getGeoServer());
        estimator.setMaxFeatures(0);

        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, estimator);

        try {
            downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                    null, // filter
                    null, // mail
                    "shape-zip", // outputFormat
                    null, // targetCRS
                    CRS.decode("EPSG:32615"), // roiCRS
                    roi, // roi
                    false, // cropToGeometry
                    new NullProgressListener() // progressListener
                    );
        } catch (ProcessException e) {
            assertEquals(
                    "Error while checking Feature Download Limits: Max allowed of 0 features exceeded.",
                    e.getMessage() + (e.getCause() != null ? ": " + e.getCause().getMessage() : ""));
            return;
        }

        assertFalse(true);
    }

    /**
     * Test download physical limit for raster data.
     * 
     * @throws Exception the exception
     */
    public void testDownloadPhysicalLimitsRaster() throws Exception {
        ProcessListener listener = new ProcessListener(new ExecutionStatus(null, "0",
                ProcessState.RUNNING, 0));

        DownloadEstimatorProcess estimator = new DownloadEstimatorProcess(getGeoServer());
        estimator.setHardOutputLimit(10);

        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, estimator);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON (( -127.57473954542964 54.06575021619523, -130.8545966116691 52.00807146727025, -129.50812897394974 49.85372324691927, -130.5300633861675 49.20465679591609, -129.25955033314003 48.60392508062591, -128.00975216684665 50.986137055052474, -125.8623089087404 48.63154492960477, -123.984159178178 50.68231871628503, -126.91186316993704 52.15307567440926, -125.3444367403868 53.54787804784162, -127.57473954542964 54.06575021619523 ))");
        roi.setSRID(4326);

        downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                null, // filter
                null, // mail
                "geotiff", // outputFormat
                null, // targetCRS
                CRS.decode("EPSG:4326", true), // roiCRS
                roi, // roi
                true, // cropToGeometry
                listener // progressListener
                );

        Throwable e = listener.exception;
        assertEquals(
                "Could not complete the Download Process: Download Exceeded the maximum HARD allowed size!",
                e.getMessage() + (e.getCause() != null ? ": " + e.getCause().getMessage() : ""));
    }

    /**
     * Test download physical limit for vectorial data.
     * 
     * @throws Exception the exception
     */
    public void testDownloadPhysicalLimitsVector() throws Exception {
        ProcessListener listener = new ProcessListener(new ExecutionStatus(null, "0",
                ProcessState.RUNNING, 0));

        DownloadEstimatorProcess estimator = new DownloadEstimatorProcess(getGeoServer());
        estimator.setHardOutputLimit(1);

        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, estimator);

        try {
            downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                    null, // filter
                    null, // mail
                    "shape-zip", // outputFormat
                    null, // targetCRS
                    CRS.decode("EPSG:32615"), // roiCRS
                    roi, // roi
                    false, // cropToGeometry
                    listener // progressListener
                    );

        } catch (ProcessException e) {
            assertEquals(
                    "java.io.IOException: Download Exceeded the maximum HARD allowed size!: Download Exceeded the maximum HARD allowed size!",
                    e.getMessage() + (e.getCause() != null ? ": " + e.getCause().getMessage() : ""));

            Throwable le = listener.exception;
            assertEquals(
                    "java.io.IOException: Download Exceeded the maximum HARD allowed size!: Download Exceeded the maximum HARD allowed size!",
                    le.getMessage()
                            + (le.getCause() != null ? ": " + le.getCause().getMessage() : ""));

            return;
        }

        assertFalse(true);
    }

    /**
     * The listener interface for receiving process events. The class that is interested in processing a process event implements this interface, and
     * the object created with that class is registered with a component using the component's <code>addProcessListener<code> method. When
     * the process event occurs, that object's appropriate
     * method is invoked.
     * 
     * @see ProcessEvent
     */
    static class ProcessListener implements ProgressListener {

        /** The Constant LOGGER. */
        static final Logger LOGGER = Logging.getLogger(ProcessListener.class);

        /** The status. */
        ExecutionStatus status;

        /** The task. */
        InternationalString task;

        /** The description. */
        String description;

        /** The exception. */
        Throwable exception;

        /**
         * Instantiates a new process listener.
         * 
         * @param status the status
         */
        public ProcessListener(ExecutionStatus status) {
            this.status = status;
        }

        /**
         * Gets the task.
         * 
         * @return the task
         */
        public InternationalString getTask() {
            return task;
        }

        /**
         * Sets the task.
         * 
         * @param task the new task
         */
        public void setTask(InternationalString task) {
            this.task = task;
        }

        /**
         * Gets the description.
         * 
         * @return the description
         */
        public String getDescription() {
            return this.description;
        }

        /**
         * Sets the description.
         * 
         * @param description the new description
         */
        public void setDescription(String description) {
            this.description = description;

        }

        /**
         * Started.
         */
        public void started() {
            status.setPhase(ProcessState.RUNNING);
        }

        /**
         * Progress.
         * 
         * @param percent the percent
         */
        public void progress(float percent) {
            status.setProgress(percent);
        }

        /**
         * Gets the progress.
         * 
         * @return the progress
         */
        public float getProgress() {
            return status.getProgress();
        }

        /**
         * Complete.
         */
        public void complete() {
            // nothing to do
        }

        /**
         * Dispose.
         */
        public void dispose() {
            // nothing to do
        }

        /**
         * Checks if is canceled.
         * 
         * @return true, if is canceled
         */
        public boolean isCanceled() {
            return status.getPhase() == ProcessState.CANCELLED;
        }

        /**
         * Sets the canceled.
         * 
         * @param cancel the new canceled
         */
        public void setCanceled(boolean cancel) {
            if (cancel == true) {
                status.setPhase(ProcessState.CANCELLED);
            }

        }

        /**
         * Warning occurred.
         * 
         * @param source the source
         * @param location the location
         * @param warning the warning
         */
        public void warningOccurred(String source, String location, String warning) {
            LOGGER.log(Level.WARNING,
                    "Got a warning during process execution " + status.getExecutionId() + ": "
                            + warning);
        }

        /**
         * Exception occurred.
         * 
         * @param exception the exception
         */
        public void exceptionOccurred(Throwable exception) {
            this.exception = exception;
        }

    }

    /**
     * Decode shape.
     * 
     * @param input the input
     * @return the object
     * @throws Exception the exception
     */
    public Object decodeShape(InputStream input) throws Exception {
        // create the temp directory and register it as a temporary resource
        File tempDir = IOUtils.createRandomDirectory(IOUtils.createTempDirectory("shpziptemp")
                .getAbsolutePath(), "wps-cluster", "download-services");

        // unzip to the temporary directory
        ZipInputStream zis = null;
        File shapeFile = null;
        File zipFile = null;

        // extract shp-zip file
        try {
            zis = new ZipInputStream(input);
            ZipEntry entry = null;

            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                File file = new File(tempDir, entry.getName());
                if (entry.isDirectory()) {
                    file.mkdir();
                } else {

                    if (file.getName().toLowerCase().endsWith(".shp")) {
                        shapeFile = file;
                    } else if (file.getName().toLowerCase().endsWith(".zip")) {
                        zipFile = file;
                    }

                    int count;
                    byte data[] = new byte[4096];
                    // write the files to the disk
                    FileOutputStream fos = null;
                    try {
                        fos = new FileOutputStream(file);
                        while ((count = zis.read(data)) != -1) {
                            fos.write(data, 0, count);
                        }
                        fos.flush();
                    } finally {
                        if (fos != null) {
                            fos.close();
                        }
                    }

                }
                zis.closeEntry();
            }
        } finally {
            if (zis != null) {
                zis.close();
            }
        }

        if (shapeFile == null) {
            if (zipFile != null)
                return decodeShape(new FileInputStream(zipFile));
            else {
                FileUtils.deleteDirectory(tempDir);
                throw new IOException("Could not find any file with .shp extension in the zip file");
            }
        } else {
            ShapefileDataStore store = new ShapefileDataStore(DataUtilities.fileToURL(shapeFile));
            return store.getFeatureSource().getFeatures();
        }
    }

}
