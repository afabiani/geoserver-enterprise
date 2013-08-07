package org.geoserver.wps.gs;

import java.net.URL;
import java.util.Date;

import org.geoserver.data.test.MockData;
import org.geoserver.test.GeoServerTestSupport;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.geometry.jts.JTS;
import org.geotools.util.NullProgressListener;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Point;

public class IDASoundPropagationModelProcessTest extends GeoServerTestSupport {
    /**
     * Try to re-import buildings as another layer (different name, different projection)
     */
    public void testIDASoundPropagationModel() throws Exception {
    	IDASoundPropagationModelProcess idaSPMProcess = new IDASoundPropagationModelProcess(getGeoServer());
		
        Point footprint = JTS.toGeometry(new GeneralDirectPosition(5.23044, 38.66605));
        
		SimpleFeatureCollection features = 
        	idaSPMProcess.execute(
        			"user_id",
        			"name",
        			new URL("http://output_url"),
        			new Date(),
        			null,
        			null,
        			null,
        			MockData.CITE_PREFIX,
        			MockData.CITE_PREFIX,
        			null,
        			"raster",
        			null,
        			"spring",
        			20.0,
        			1.0,
        			2.0,
        			null,
        			null,
        			null,
        			null,
        			null,
        			null,
        			null,
        			footprint,
        			new NullProgressListener()
        			);
        
        assertNotNull(features);
        
        assertEquals(1, features.size());
        
        SimpleFeature feature = features.features().next();
        
        assertNotNull(feature);
    }
}
