package org.geoserver.wps.gs.utils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.geotools.data.FeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.util.logging.Logging;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryComponentFilter;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.precision.EnhancedPrecisionOp;

public class GeometryUtilis {

	protected static final Logger LOGGER = Logging.getLogger(GeometryUtilis.class);

	private GeometryUtilis() {
	}

	public static Geometry union(Geometry a, Geometry b) {
		return reduce(EnhancedPrecisionOp.union(a, b));
	}

	public static Geometry difference(Geometry a, Geometry b) {
		return reduce(EnhancedPrecisionOp.difference(a, b));
	}

	/**
	 * Reduce a GeometryCollection to a MultiPolygon. This method basically
	 * explores the collection and assembles all the linear rings and polygons
	 * into a multipolygon. The idea there is that contains() works on a multi
	 * polygon but not a collection. If we throw out points and lines, etc, we
	 * should still be OK. This is not 100% correct, but we should still be able
	 * to throw away some features which is the point of all this.
	 * 
	 * @param geometry
	 * @return
	 */
	static Geometry reduce(Geometry geometry) {
		if (geometry instanceof GeometryCollection) {

			if (geometry instanceof MultiPolygon) {
				return geometry;
			}

			// WKTWriter wktWriter = new WKTWriter();
			// logger.warn("REDUCING COLLECTION: " + wktWriter.write(geometry));

			final ArrayList<Polygon> polygons = new ArrayList<Polygon>();
			final GeometryFactory factory = geometry.getFactory();

			geometry.apply(new GeometryComponentFilter() {
				public void filter(Geometry geom) {
					if (geom instanceof LinearRing) {
						polygons.add(factory.createPolygon((LinearRing) geom,
								null));
					} else if (geom instanceof LineString) {
						// what to do?
					} else if (geom instanceof Polygon) {
						polygons.add((Polygon) geom);
					}
				}
			});

			MultiPolygon multiPolygon = factory.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
			multiPolygon.normalize();

			return multiPolygon;
		}

		return geometry;
	}

	/******
     * this method takes a zip file name and return its SimpleFeatureCollection
     *
     * @param filename
     *            the full name of the shape file
	 * @param URI_URL 
     * @return the simple feature collection
     */
    static public SimpleFeatureCollection simpleFeatureCollectionByShp(String filename, String URI_URL)
    {
        if (filename == null)
        {
            LOGGER.severe("Shapefile name is null!");

            return null;
        }

        File shpfile = new File(filename);
        if (!shpfile.exists() || !shpfile.canRead())
        {
            return null;
        }

        try
        {
            ShapefileDataStore store = new ShapefileDataStore(shpfile.toURI().toURL(), new URI(URI_URL), true, true, ShapefileDataStore.DEFAULT_STRING_CHARSET);
            FeatureSource fs = store.getFeatureSource();

            return (SimpleFeatureCollection) fs.getFeatures();
        }
        catch (Exception e1)
        {
            LOGGER.severe(e1.getLocalizedMessage());

            return null;
        }

    }
}