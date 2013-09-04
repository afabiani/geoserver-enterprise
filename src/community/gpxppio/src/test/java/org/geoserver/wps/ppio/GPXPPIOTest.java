package org.geoserver.wps.ppio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.custommonkey.xmlunit.SimpleNamespaceContext;
import org.custommonkey.xmlunit.XMLUnit;
import org.custommonkey.xmlunit.XpathEngine;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.config.ContactInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.config.GeoServerInfo;
import org.geoserver.config.SettingsInfo;
import org.geoserver.data.test.MockData;
import org.geoserver.test.GeoServerTestSupport;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.w3c.dom.Document;

public class GPXPPIOTest extends GeoServerTestSupport {

    private GPXPPIO ppio;
    private XpathEngine xpath;
    
    @Override
    protected void oneTimeSetUp() throws Exception {
        super.oneTimeSetUp();
        
        // init xmlunit
        Map<String, String> namespaces = new HashMap<String, String>();
        namespaces.put("", "http://www.topografix.com/GPX/1/1");
        namespaces.put("gpx", "http://www.topografix.com/GPX/1/1");
        namespaces.put("att", "http://www.geoserver.org");
        XMLUnit.setXpathNamespaceContext(new SimpleNamespaceContext(namespaces));
        xpath = XMLUnit.newXpathEngine();
    }

    @Override
    protected void setUpInternal() throws Exception {
        GeoServer gs = getGeoServer();
        GeoServerInfo global = gs.getGlobal();
        SettingsInfo settings = global.getSettings();
        ContactInfo contact = settings.getContact();
        contact.setContactOrganization("GeoServer");
        contact.setOnlineResource("http://www.geoserver.org");
        gs.save(global);
        
        ppio = new GPXPPIO(gs);
    }
    
    public void testEncodePolygon() throws IOException {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(getLayerId(MockData.BASIC_POLYGONS));
        SimpleFeatureCollection fc = (SimpleFeatureCollection) fti.getFeatureSource(null, null).getFeatures();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ppio.encode(fc, bos);
            fail("Should have thrown an exception");
        } catch(IOException e) {
            assert(e.getCause() instanceof IllegalArgumentException);
        }
    }
    
    public void testEncodeLinestring() throws Exception {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(getLayerId(MockData.ROAD_SEGMENTS));
        SimpleFeatureCollection fc = (SimpleFeatureCollection) fti.getFeatureSource(null, null).getFeatures();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ppio.encode(fc, bos);
        Document dom = dom(new ByteArrayInputStream(bos.toByteArray()));
        // print(dom);
        checkValidationErorrs(dom, "./src/test/resources/org/geoserver/wps/ppio/gpx.xsd");
        
        assertEquals("GeoServer", xpath.evaluate("/gpx:gpx/@creator", dom));
        assertEquals("GeoServer", xpath.evaluate("/gpx:gpx/gpx:metadata/gpx:link/gpx:text", dom));
        assertEquals("http://www.geoserver.org", xpath.evaluate("/gpx:gpx/gpx:metadata/gpx:link/@href", dom));
        assertEquals(5, xpath.getMatchingNodes("/gpx:gpx/gpx:trk", dom).getLength());
        assertEquals("102", xpath.evaluate("/gpx:gpx/gpx:trk[1]/gpx:extensions/att:FID", dom));
        assertEquals("Route 5", xpath.evaluate("/gpx:gpx/gpx:trk[1]/gpx:extensions/att:NAME", dom));
    }
    
    public void testEncodePoints() throws Exception {
        FeatureTypeInfo fti = getCatalog().getFeatureTypeByName(getLayerId(MockData.POINTS));
        SimpleFeatureCollection fc = (SimpleFeatureCollection) fti.getFeatureSource(null, null).getFeatures();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ppio.encode(fc, bos);
        Document dom = dom(new ByteArrayInputStream(bos.toByteArray()));
        // print(dom);
        checkValidationErorrs(dom, "./src/test/resources/org/geoserver/wps/ppio/gpx.xsd");
        assertEquals("GeoServer", xpath.evaluate("/gpx:gpx/@creator", dom));
        assertEquals("GeoServer", xpath.evaluate("/gpx:gpx/gpx:metadata/gpx:link/gpx:text", dom));
        assertEquals("http://www.geoserver.org", xpath.evaluate("/gpx:gpx/gpx:metadata/gpx:link/@href", dom));
        assertEquals(1, xpath.getMatchingNodes("/gpx:gpx/gpx:wpt", dom).getLength());
        assertEquals("t0000", xpath.evaluate("/gpx:gpx/gpx:wpt[1]/gpx:extensions/att:id", dom));
    }
    
}
