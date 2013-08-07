package org.geoserver.wps.gs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.geotools.filter.v1_0.OGCConfiguration;
import org.geotools.xml.Configuration;
import org.geotools.xml.Parser;
import org.opengis.filter.Filter;
import org.xml.sax.SAXException;

public class FilterEncodingTest {

	static final Configuration xml = new OGCConfiguration();
	
	/**
	 * @param args
	 * @throws ParserConfigurationException 
	 * @throws SAXException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
		Parser p = new Parser(xml);

		String filterXml = 
		"<ogc:Filter xmlns:ogc=\"http://www.opengis.net/ogc\">" +
		"	<ogc:Or>" +
		"		<ogc:PropertyIsGreaterThanOrEqualTo>" +
		"			<ogc:PropertyName>TEEST_ASCIIgrid</ogc:PropertyName>" +
		"			<ogc:Literal>0</ogc:Literal>" +
		"		</ogc:PropertyIsGreaterThanOrEqualTo>" +
		"		<ogc:PropertyIsLessThanOrEqualTo>" +
		"			<ogc:PropertyName>TEEST_ASCIIgrid</ogc:PropertyName>" +
		"			<ogc:Literal>250</ogc:Literal>" +
		"		</ogc:PropertyIsLessThanOrEqualTo>" +
		"	</ogc:Or>" +
		"</ogc:Filter>";
		
		Object filter = p.parse(new ByteArrayInputStream(filterXml.getBytes()));
		
		System.out.println(((Filter)filter).toString());
	}

}
