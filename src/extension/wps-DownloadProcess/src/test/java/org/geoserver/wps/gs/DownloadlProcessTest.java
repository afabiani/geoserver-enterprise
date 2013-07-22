package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
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

public class DownloadlProcessTest extends GeoServerTestSupport {

    @Override
    protected void populateDataDirectory(MockData dataDirectory) throws Exception {
        super.populateDataDirectory(dataDirectory);

        dataDirectory.addWcs10Coverages();
    }

    /**
     * Test download of vectorial data
     */

    final static Polygon roi;
    static {
        try {
            roi = (Polygon) new WKTReader2()
                    .read("POLYGON (( 500116.08576537756 499994.25579707103, 500116.08576537756 500110.1012210889, 500286.2657688021 500110.1012210889, 500286.2657688021 499994.25579707103, 500116.08576537756 499994.25579707103 ))");
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void testGetFeaturesAsShapefile() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POLYGONS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        File shpeZip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "shape-zip", // outputFormat
                CRS.decode("EPSG:32615"), // targetCRS
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

    public void testFilteredClippedFeatures() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON ((0.0008993124415341 0.0006854377923293, 0.0008437876520112 0.0006283489242283, 0.0008566913002806 0.0005341131898971, 0.0009642217025257 0.0005188634237605, 0.0011198475210477 0.000574779232928, 0.0010932581852198 0.0006572843779233, 0.0008993124415341 0.0006854377923293))");

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.BUILDINGS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        File shpeZip = downloadProcess.execute(getLayerId(MockData.BUILDINGS), // layerName
                "ADDRESS = '123 Main Street'", // filter
                null, // mail
                "shape-zip", // outputFormat
                DefaultGeographicCRS.WGS84, // targetCRS
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

    public Object decodeShape(InputStream input) throws Exception {
        // create the temp directory and register it as a temporary resource
        File tempDir = IOUtils.createTempDirectory("shpziptemp");

        // unzip to the temporary directory
        ZipInputStream zis = null;
        File shapeFile = null;

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
            FileUtils.deleteDirectory(tempDir);
            throw new IOException("Could not find any file with .shp extension in the zip file");
        } else {
            ShapefileDataStore store = new ShapefileDataStore(DataUtilities.fileToURL(shapeFile));
            return store.getFeatureSource().getFeatures();
        }
    }

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
                CRS.decode("EPSG:32615"), // targetCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(gml2Zip);

        SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) new WFSPPIO.WFS10()
                .decode(new FileInputStream(gml2Zip));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());

        // GML 3
        File gml3Zip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "application/gml-3.1.1", // outputFormat
                CRS.decode("EPSG:32615"), // targetCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(gml3Zip);

        rawTarget = (SimpleFeatureCollection) new WFSPPIO.WFS11().decode(new FileInputStream(
                gml3Zip));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());
    }

    public void testGetFeaturesAsGeoJSON() throws Exception {
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);

        FeatureTypeInfo ti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POLYGONS));
        SimpleFeatureCollection rawSource = (SimpleFeatureCollection) ti.getFeatureSource(null,
                null).getFeatures();

        File jsonZip = downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                null, // filter
                null, // mail
                "json", // outputFormat
                CRS.decode("EPSG:32615"), // targetCRS
                roi, // roi
                false, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(jsonZip);

        SimpleFeatureCollection rawTarget = (SimpleFeatureCollection) new FeatureJSON()
                .readFeatureCollection(new FileInputStream(jsonZip));

        assertNotNull(rawTarget);

        assertEquals(rawSource.size(), rawTarget.size());
    }

    /**
     * Test download of raster data
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
                CRS.decode("EPSG:4326", true), // targetCRS
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
                roiResampled, // roi
                true, // cropToGeometry
                new NullProgressListener() // progressListener
                );

        assertNotNull(resampledZip);

        GeoTiffReader reader = null;
        GridCoverage2D gc = null, gcResampled = null;
        try {
            reader = new GeoTiffReader(rasterZip);
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
            reader = new GeoTiffReader(resampledZip);
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
     * PPIO Test
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
                CRS.decode("EPSG:4326"), // targetCRS
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
     * Test download estimator for raster data
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
                    CRS.decode("EPSG:4326", true), // targetCRS
                    roi, // roi
                    true, // cropToGeometry
                    new NullProgressListener() // progressListener
                    );
        } catch (ProcessException e) {
            assertEquals(
                    "This request is trying to read too much data, the limit is 10B but the actual amount of bytes to be read is 11,32MB",
                    e.getMessage());
            return;
        }

        assertFalse(true);
    }

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
                    CRS.decode("EPSG:4326", true), // targetCRS
                    roi, // roi
                    true, // cropToGeometry
                    new NullProgressListener() // progressListener
                    );
        } catch (ProcessException e) {
            assertEquals(
                    "This request is trying to generate too much data, the limit is 10B but the actual amount of bytes to be written in the output is 10,86MB",
                    e.getMessage());
            return;
        }

        assertFalse(true);
    }

    /**
     * Test download estimator for vectorial data
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
                    CRS.decode("EPSG:32615"), // targetCRS
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
     * Test download physical limit for raster data
     */
    public void testDownloadPhysicalLimitsRaster() throws Exception {
        ProcessListener listener = new ProcessListener(new ExecutionStatus(null, "0",
                ProcessState.RUNNING, 0));
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);
        downloadProcess.setHardOutputLimit(10);

        Polygon roi = (Polygon) new WKTReader2()
                .read("POLYGON (( -127.57473954542964 54.06575021619523, -130.8545966116691 52.00807146727025, -129.50812897394974 49.85372324691927, -130.5300633861675 49.20465679591609, -129.25955033314003 48.60392508062591, -128.00975216684665 50.986137055052474, -125.8623089087404 48.63154492960477, -123.984159178178 50.68231871628503, -126.91186316993704 52.15307567440926, -125.3444367403868 53.54787804784162, -127.57473954542964 54.06575021619523 ))");
        roi.setSRID(4326);

        downloadProcess.execute(getLayerId(MockData.USA_WORLDIMG), // layerName
                null, // filter
                null, // mail
                "geotiff", // outputFormat
                CRS.decode("EPSG:4326", true), // targetCRS
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
     * Test download physical limit for vectorial data
     */
    public void testDownloadPhysicalLimitsVector() throws Exception {
        ProcessListener listener = new ProcessListener(new ExecutionStatus(null, "0",
                ProcessState.RUNNING, 0));
        DownloadProcess downloadProcess = new DownloadProcess(getGeoServer(), null, null);
        downloadProcess.setHardOutputLimit(1);

        try {
            downloadProcess.execute(getLayerId(MockData.POLYGONS), // layerName
                    null, // filter
                    null, // mail
                    "shape-zip", // outputFormat
                    CRS.decode("EPSG:32615"), // targetCRS
                    roi, // roi
                    false, // cropToGeometry
                    listener // progressListener
                    );

        } catch (ProcessException e) {
            assertEquals(
                    "Could not complete the Download Process: Download Exceeded the maximum HARD allowed size!",
                    e.getMessage() + (e.getCause() != null ? ": " + e.getCause().getMessage() : ""));

            Throwable le = listener.exception;
            assertEquals(
                    "Could not complete the Download Process: Download Exceeded the maximum HARD allowed size!",
                    le.getMessage()
                            + (le.getCause() != null ? ": " + le.getCause().getMessage() : ""));

            return;
        }

        assertFalse(true);
    }

    static class ProcessListener implements ProgressListener {

        static final Logger LOGGER = Logging.getLogger(ProcessListener.class);

        ExecutionStatus status;

        InternationalString task;

        String description;

        Throwable exception;

        public ProcessListener(ExecutionStatus status) {
            this.status = status;
        }

        public InternationalString getTask() {
            return task;
        }

        public void setTask(InternationalString task) {
            this.task = task;
        }

        public String getDescription() {
            return this.description;
        }

        public void setDescription(String description) {
            this.description = description;

        }

        public void started() {
            status.setPhase(ProcessState.RUNNING);
        }

        public void progress(float percent) {
            status.setProgress(percent);
        }

        public float getProgress() {
            return status.getProgress();
        }

        public void complete() {
            // nothing to do
        }

        public void dispose() {
            // nothing to do
        }

        public boolean isCanceled() {
            return status.getPhase() == ProcessState.CANCELLED;
        }

        public void setCanceled(boolean cancel) {
            if (cancel == true) {
                status.setPhase(ProcessState.CANCELLED);
            }

        }

        public void warningOccurred(String source, String location, String warning) {
            LOGGER.log(Level.WARNING,
                    "Got a warning during process execution " + status.getExecutionId() + ": "
                            + warning);
        }

        public void exceptionOccurred(Throwable exception) {
            this.exception = exception;
        }

    }
}
