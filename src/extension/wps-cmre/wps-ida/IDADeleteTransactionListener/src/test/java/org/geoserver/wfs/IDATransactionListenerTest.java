/* Copyright (c) 2012 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wfs;

import javax.xml.namespace.QName;

import org.geoserver.wps.gs.IDARasterAlgebraProcess;

/**
 * This test must be run with the server configured with the wfs 1.0 cite
 * configuration, with data initialized.
 * 
 * @author Justin Deoliveira, The Open Planning Project
 * 
 */
public class IDATransactionListenerTest extends WFSTestSupport {
    
//	public static QName IDA_RASTER_GEOMETRIES = new QName(SystemTestData.CITE_URI, IDARasterAlgebraProcess.DEFAULT_TYPE_NAME, SystemTestData.CITE_PREFIX);
	
	IDADeleteTransactionListener listener;

//    @Override
//    protected void setUpSpring(List<String> springContextLocations) {    	
//        super.setUpSpring(springContextLocations);
//        springContextLocations.add("classpath:/org/geoserver/wfs/TransactionListenerTestContext.xml");
//    }
        
//    @Before
//    public void clearState() throws Exception {
//        listener = (IDADeleteTransactionListener) applicationContext.getBean("nurcIDADeleteTransactionListener");
//        listener.clear();
//    }

//    @Override
//    protected void setUpInternal(SystemTestData dataDirectory) throws Exception {
//    	dataDirectory.addVectorLayer(IDA_RASTER_GEOMETRIES, Collections.EMPTY_MAP, getClass(), getCatalog());
//    }
    
//    @Test
//    public void testDelete() throws Exception {
//        // perform a delete
//        String delete = "<wfs:Transaction service=\"WFS\" version=\"1.0.0\" "
//                + "xmlns:cgf=\"http://www.opengis.net/cite/geometry\" "
//                + "xmlns:ogc=\"http://www.opengis.net/ogc\" "
//                + "xmlns:wfs=\"http://www.opengis.net/wfs\"> "
//                + "<wfs:Delete typeName=\"cite:"+IDARasterAlgebraProcess.DEFAULT_TYPE_NAME+"\"> " + "<ogc:Filter> "
//                + "<ogc:PropertyIsEqualTo> "
//                + "<ogc:PropertyName>cite:attributeName</ogc:PropertyName> "
//                + "<ogc:Literal>test_00</ogc:Literal> "
//                + "</ogc:PropertyIsEqualTo> " + "</ogc:Filter> "
//                + "</wfs:Delete> " + "</wfs:Transaction>";
//
//        postAsDOM("wfs", delete);
//        assertEquals(1, listener.events.size());
//        TransactionEvent event = (TransactionEvent) listener.events.get(0);
//        assertTrue(event.getSource() instanceof DeleteElementType);
//        assertEquals(TransactionEventType.PRE_DELETE, event.getType());
//        assertEquals(IDA_RASTER_GEOMETRIES, event.getLayerName());
//        assertEquals(1, listener.features.size());
//        Feature deleted = (Feature) listener.features.get(0);
//        assertEquals("test_00", deleted.getProperty("attributeName").getValue());
//    }

}
