import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.geoserver.wps.gs.IDARasterAlgebraProcess;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.property.PropertyDataStore;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier;
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier;


public class PropertyImporter {

    public static void main(String[] args) throws IOException {
        Map<Serializable, Object> params = new HashMap<Serializable, Object>();
        params.put(JDBCDataStoreFactory.USER.key, "postgres");
        params.put(JDBCDataStoreFactory.PASSWD.key, "postgres");
        params.put(JDBCDataStoreFactory.HOST.key, "localhost");
        params.put(JDBCDataStoreFactory.PORT.key, PostgisNGDataStoreFactory.PORT.sample);
        params.put(JDBCDataStoreFactory.DATABASE.key, "nurc-ida");
        params.put(JDBCDataStoreFactory.DBTYPE.key, "postgis");
        
        DataStore postgis = new PostgisNGDataStoreFactory().createDataStore(params);
        if(postgis != null && postgis.getTypeNames() != null)
            System.out.println("Postgis connected");
        
        FeatureStore oraStore = (FeatureStore) postgis.getFeatureSource(IDARasterAlgebraProcess.DEFAULT_TYPE_NAME);
        
        SimpleFeatureType targetSchema = (SimpleFeatureType) oraStore.getSchema();
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(targetSchema);
        
        FeatureIterator fi = oraStore.getFeatures(new Query(IDARasterAlgebraProcess.DEFAULT_TYPE_NAME, Filter.INCLUDE, 10, (String[]) null, null)).features();
        
        PropertyDataStore pds = new PropertyDataStore(new File("c:/temp"));
        pds.createSchema(targetSchema);
        SimpleFeatureStore pStore = (SimpleFeatureStore) pds.getFeatureSource(IDARasterAlgebraProcess.DEFAULT_TYPE_NAME);
        
        Transaction t = new DefaultTransaction();
        oraStore.setTransaction(t);
        while(fi.hasNext()) {
            SimpleFeature source = (SimpleFeature) fi.next();
        
            for(AttributeDescriptor ad : targetSchema.getAttributeDescriptors()) {
                String attribute = ad.getLocalName();
                Object value = source.getAttribute(attribute);
                if(value instanceof Geometry) {
                    Geometry g = (Geometry) value;
                    value = TopologyPreservingSimplifier.simplify(g, 0.1);
                }
                builder.set(attribute, value);
            }
            
            pStore.addFeatures(DataUtilities.collection(builder.buildFeature(null)));
        }
        t.commit();
        t.close();
        
    }
}