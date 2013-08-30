package org.geoserver.wps.ppio;

import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;

/**
 * Encoder class to encode SimpleFeatureCollection to GPX
 * The encoder uses only a XMLStreamWriter for simplicity and
 * performance sake.
 *   
 * @author Peter Hopfgartner (R3 GIS)
 */
public class GpxEncoder {
	boolean writeExtendedData = false;

	Map<String, Class> trkAttributes = new HashMap<String, Class>();
	Map<String, Class> wptAttributes = new HashMap<String, Class>();
	Map<String, Class> rteAttributes = new HashMap<String, Class>();
	
	public GpxEncoder() {
		trkAttributes.put("name", String.class);
		trkAttributes.put("desc", String.class);
		
		trkAttributes.put("name", String.class);
		trkAttributes.put("desc", String.class);
		
	}
	
	private Map<String, String> types = new HashMap<String, String>();

	public void encode(OutputStream lFileOutputStream,
			SimpleFeatureCollection collection) throws XMLStreamException,
			NoSuchAuthorityCodeException, FactoryException {

		CRSAuthorityFactory crsFactory = CRS.getAuthorityFactory(true);

		CoordinateReferenceSystem targetCRS = crsFactory
				.createCoordinateReferenceSystem("EPSG:4326");
		collection = new ReprojectingFeatureCollection(collection, targetCRS);

		XMLOutputFactory xmlFactory = XMLOutputFactory.newInstance();
		XMLStreamWriter writer = xmlFactory
				.createXMLStreamWriter(lFileOutputStream);
		writer.writeStartDocument();
		writer.writeStartElement("gpx");
		writer.writeAttribute("xmlns", "http://www.topografix.com/GPX/1/1");
		writer.writeAttribute("version", "1.1");
		writer.writeAttribute("creator", "Autonome Provinz Bozen - Provincia Autonoma di Bolzano");
		
		writer.writeStartElement("metadata");
		
		writer.writeStartElement("link");
		writer.writeAttribute("href", "http://sdi.provinz.bz.it");
		writer.writeStartElement("text");
		writer.writeCharacters("SDI Autonome Provinz Bozen - Provincia Autonoma di Bolzano");		
		writer.writeEndElement();
		writer.writeEndElement();
		
		writer.writeStartElement("time");
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		writer.writeCharacters(sdf.format(calendar.getTime()));		
		writer.writeEndElement();
		
		writer.writeEndElement(); // metadata
		
		String schemaName = "";

		FeatureIterator<SimpleFeature> iter = collection.features();

		try {
			while (iter.hasNext()) {
				SimpleFeature f = iter.next();
				
				Geometry g = (Geometry) f.getDefaultGeometry();
				if (g instanceof MultiLineString) {
					MultiLineString mls = (MultiLineString) g;
					int numGeometries = mls.getNumGeometries();
					writer.writeStartElement("trk");
					for (int i = 0; i < numGeometries; i++) {
						LineString ls = (LineString) mls.getGeometryN(i);
						writeTrkSeg(writer, ls);
					}
					writer.writeEndElement();
				} else if (g instanceof LineString) {
					writeRte(writer, (LineString) g);
				} else if (g instanceof MultiPoint) {
					MultiPoint mpt = (MultiPoint) g;
					int numGeometries = mpt.getNumGeometries();
					for (int i = 0; i < numGeometries; i++) {
						Point pt = (Point) mpt.getGeometryN(i);
						writeWpt(writer, pt);
					}
				} else if (g instanceof Point) {
					writeWpt(writer, (Point) g);
				} else {
					System.out.println(g.getClass().getName()
							+ "not implemented");
					continue;
				}
				if (writeExtendedData) {
					writeData(writer, f);
				}
			}
		} finally {
			collection.close(iter);
		}

		writer.writeEndDocument(); // </kml>
		return;
		/*
		*/
	}

	private void writeCoordinates(XMLStreamWriter writer, String ptElementName, LineString ls)
			throws XMLStreamException {
		Coordinate[] coordinates = ls.getCoordinates();
		for (int ic = 0; ic < coordinates.length; ic++) {
			writeWpt(writer, ptElementName, coordinates[ic].x, coordinates[ic].y, coordinates[ic].z);
		}
	}
	
	private void writeWpt(XMLStreamWriter writer, String ptElementName, double x, double y, double z)
			throws XMLStreamException {
		writer.writeStartElement(ptElementName);
		writer.writeAttribute("lat", Double.toString(y));
		writer.writeAttribute("lon", Double.toString(x));
		if (!Double.isNaN(z)) {
			writer.writeAttribute("ele", Double.toString(z));
		}
		writer.writeEndElement();
	}
	
	private void writeTrkSeg(XMLStreamWriter writer, LineString ls)
			throws XMLStreamException {
		writer.writeStartElement("trkseg");
		writeCoordinates(writer, "trkpt", ls);
		writer.writeEndElement();
	}
	
	private void writeRte(XMLStreamWriter writer, LineString ls)
			throws XMLStreamException {
		writer.writeStartElement("rte");
		writeCoordinates(writer, "rtept", ls);
		writer.writeEndElement();
	}
	
	private void writeWpt(XMLStreamWriter writer, Point pt)
			throws XMLStreamException {
		writer.writeStartElement("wpt");
		Coordinate c = pt.getCoordinate();
		writer.writeAttribute("lon", Double.toString(c.x));
		writer.writeAttribute("lat", Double.toString(c.y));
		if (!Double.isNaN(c.z)) {
			writer.writeAttribute("ele", Double.toString(c.z));
		}
		writer.writeEndElement();
	}

	private void writeData(XMLStreamWriter writer, SimpleFeature f)
			throws XMLStreamException {
		writer.writeStartElement("ExtendedData");
		for (Property p : f.getProperties()) {
			Name name = p.getName();
			if (!(p.getValue() instanceof Geometry) && p.getValue() != null) {
				writer.writeStartElement("Data");
				writer.writeAttribute("name", name.getLocalPart());
				writer.writeStartElement("value");
				writer.writeCharacters(p.getValue().toString());
				writer.writeEndElement();
				writer.writeEndElement();
			}
		}
		writer.writeEndElement();
	}

}
