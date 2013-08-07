package org.geoserver.wps.gs;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.geoserver.catalog.AttributionInfo;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.ppio.FeatureAttribute;
import org.geoserver.wps.raster.algebra.RasterAlgebraProcess;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.imageio.GeoToolsWriteParams;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.gce.geotiff.GeoTiffReader;
import org.geotools.gce.geotiff.GeoTiffWriteParams;
import org.geotools.gce.geotiff.GeoTiffWriter;
import org.geotools.geometry.jts.GeometryBuilder;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.referencing.CRS;
import org.geotools.util.NullProgressListener;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.parameter.GeneralParameterValue;
import org.opengis.parameter.ParameterValue;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Geometry;

/**
 * EXAMPLE REQUEST:
 </BR></BR>
<CODE>
&lt;?xml version="1.0" encoding="UTF-8"?&gt;
&lt;wps:Execute version="1.0.0" service="WPS" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.opengis.net/wps/1.0.0" xmlns:wfs="http://www.opengis.net/wfs" xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:gml="http://www.opengis.net/gml" xmlns:ogc="http://www.opengis.net/ogc" xmlns:wcs="http://www.opengis.net/wcs/1.1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd"&gt;
  &lt;ows:Identifier&gt;gs:IDARasterAlgebra&lt;/ows:Identifier&gt;
  &lt;wps:DataInputs&gt;
    &lt;wps:Input&gt;
      &lt;ows:Identifier&gt;attributeName&lt;/ows:Identifier&gt;
      &lt;wps:Data&gt;
        &lt;wps:LiteralData&gt;test_01&lt;/wps:LiteralData&gt;
      &lt;/wps:Data&gt;
    &lt;/wps:Input&gt;
    &lt;wps:Input&gt;
      &lt;ows:Identifier&gt;runBegin&lt;/ows:Identifier&gt;
      &lt;wps:Data&gt;
        &lt;wps:LiteralData&gt;2013-01-11&lt;/wps:LiteralData&gt;
      &lt;/wps:Data&gt;
    &lt;/wps:Input&gt;
    &lt;wps:Input&gt;
      &lt;ows:Identifier&gt;wsName&lt;/ows:Identifier&gt;
      &lt;wps:Data&gt;
        &lt;wps:LiteralData&gt;nurc&lt;/wps:LiteralData&gt;
      &lt;/wps:Data&gt;
    &lt;/wps:Input&gt;
    &lt;wps:Input&gt;
      &lt;ows:Identifier&gt;storeName&lt;/ows:Identifier&gt;
      &lt;wps:Data&gt;
        &lt;wps:LiteralData&gt;nurc&lt;/wps:LiteralData&gt;
      &lt;/wps:Data&gt;
    &lt;/wps:Input&gt;
    &lt;wps:Input&gt;
      &lt;ows:Identifier&gt;classification&lt;/ows:Identifier&gt;
      &lt;wps:Data&gt;
        &lt;wps:LiteralData&gt;UNCLASSIFIED&lt;/wps:LiteralData&gt;
      &lt;/wps:Data&gt;
    &lt;/wps:Input&gt;
    &lt;wps:Input&gt;
      &lt;ows:Identifier&gt;attributeFilter&lt;/ows:Identifier&gt;
      &lt;wps:Data&gt;
        &lt;wps:ComplexData mimeType="text/plain; subtype=cql"&gt;&lt;![CDATA[nurc:srtm_39_04_2 &lt;=1 OR nurc:srtm_39_04_2 &gt;=1000]]&gt;&lt;/wps:ComplexData&gt;
      &lt;/wps:Data&gt;
    &lt;/wps:Input&gt;
  &lt;/wps:DataInputs&gt;
  &lt;wps:ResponseForm&gt;
    &lt;wps:RawDataOutput mimeType="text/xml; subtype=wfs-collection/1.0"&gt;
      &lt;ows:Identifier&gt;result&lt;/ows:Identifier&gt;
    &lt;/wps:RawDataOutput&gt;
  &lt;/wps:ResponseForm&gt;
&lt;/wps:Execute&gt;
</CODE>
 **/

@DescribeProcess(title = "IDA Raster Algebra Process", description = "Raster Algebra using OGC filters.")
public class IDARasterAlgebraProcess implements GSProcess {

	protected static final Logger LOGGER = Logging.getLogger(IDARasterAlgebraProcess.class);
	
	protected GeoServer geoServer;
	
	protected Catalog catalog;
	
	protected FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
	
	protected GeometryBuilder geomBuilder = new GeometryBuilder();

	public static final String DEFAULT_TYPE_NAME = "IDARasterAlgebraProcess";

	public IDARasterAlgebraProcess(GeoServer geoServer) {
		this.geoServer = geoServer;
		this.catalog = geoServer.getCatalog();
	}
	
	@DescribeResult(name = "result", description = "List of attributes to be converted to a FeatureType")
	public SimpleFeatureCollection execute(
			@DescribeParameter(name = "attributeName", min = 1, description = "RasterAlgebra attribute attributeName") String name,
			@DescribeParameter(name = "outputUrl", min = 1, description = "SPM attribute outputUrl") URL outputUrl,
			@DescribeParameter(name = "runBegin", min = 1, description = "SPM attribute runBegin") Date runBegin,
			@DescribeParameter(name = "runEnd", min = 0, description = "SPM attribute runEnd") Date runEnd,
			@DescribeParameter(name = "itemStatus", min = 0, description = "SPM attribute item_status") String itemStatus,
			@DescribeParameter(name = "itemStatusMessage", min = 0, description = "SPM attribute itemStatusMessage") String itemStatusMessage,
			@DescribeParameter(name = "srcPath", min = 0, description = "SPM attribute src_path") String srcPath,
			@DescribeParameter(name = "wsName", min = 1, description = "RasterAlgebra attribute workspaceName") String wsName,
			@DescribeParameter(name = "storeName", min = 1, description = "RasterAlgebra attribute storeName") String storeName,
			@DescribeParameter(name = "layerName", min = 0, description = "RasterAlgebra attribute layerName") String layerName,
			@DescribeParameter(name = "styleName", min = 0, description = "RasterAlgebra attribute styleName") String styleName,
			@DescribeParameter(name = "classification", description = "RasterAlgebra attribute classification") String classification,
			@DescribeParameter(name = "areaOfInterest", min = 0, description = "RasterAlgebra attribute ROI") Geometry areaOfInterest,
			@DescribeParameter(name = "attributeFilter", description = "RasterAlgebra attribute attributeFilter") String attributeFilter,
			ProgressListener progressListener) throws ProcessException {

		// first off, decide what is the target store
		WorkspaceInfo ws;
		if (wsName != null) {
			ws = catalog.getWorkspaceByName(wsName);
			if (ws == null) {
				throw new ProcessException("Could not find workspace "
						+ wsName);
			}
		} else {
			ws = catalog.getDefaultWorkspace();
			if (ws == null) {
				throw new ProcessException(
						"The catalog is empty, could not find a default workspace");
			}
		}
		
		/**
		 * Convert the spread attributes into a FeatureType
		 */
		List<FeatureAttribute> attributes = new ArrayList<FeatureAttribute>();

		ToFeature toFeatureProcess = new ToFeature();

		CoordinateReferenceSystem crs;
		try {
			crs = CRS.decode("EPSG:4326");
		} catch (NoSuchAuthorityCodeException e) {
			if (progressListener != null)
			{
				progressListener.exceptionOccurred(e);
			}
			throw new ProcessException("Could not DECODE the footprint CRS due to an Exception.", e);
		} catch (FactoryException e) {
			if (progressListener != null)
			{
				progressListener.exceptionOccurred(e);
			}
			throw new ProcessException("Could not DECODE the footprint CRS due to an Exception.", e);
		}

		UUID uuid = UUID.randomUUID();
		attributes.add(new FeatureAttribute("ftUUID", uuid.toString()));
		attributes.add(new FeatureAttribute("attributeName", name));
		attributes.add(new FeatureAttribute("outputUrl", outputUrl.toExternalForm()));
		attributes.add(new FeatureAttribute("runBegin", runBegin));
		attributes.add(new FeatureAttribute("runEnd", (runEnd!=null?runEnd:new Date())));
		attributes.add(new FeatureAttribute("itemStatus", (itemStatus != null ? itemStatus : "RUNNING")));
		attributes.add(new FeatureAttribute("itemStatusMessage", (itemStatusMessage != null ? itemStatusMessage : "Instrumented by Server")));
		attributes.add(new FeatureAttribute("wsName", wsName));
		attributes.add(new FeatureAttribute("storeName", (storeName != null ? storeName : "")));
		attributes.add(new FeatureAttribute("layerName", (layerName != null ? layerName : "")));
		attributes.add(new FeatureAttribute("styleName", (styleName != null ? styleName : "")));
		attributes.add(new FeatureAttribute("srcPath", (srcPath != null ? srcPath : "")));
		attributes.add(new FeatureAttribute("classification", (classification != null ? classification : "")));
		attributes.add(new FeatureAttribute("attributeFilter", (attributeFilter != null ? attributeFilter.toString() : "")));

		Geometry geometry = geomBuilder.point(0, 0);
		SimpleFeatureCollection features = toFeatureProcess.execute(geometry, crs, DEFAULT_TYPE_NAME, attributes, null);
		
		if (progressListener != null)
		{
			progressListener.progress(20);
		}
		
		if (features == null || features.isEmpty())
		{
			throw new ProcessException("There was an error while converting attributes into FeatureType.");
		}

		/**
		 * LOG into the DB
		 */
        Filter filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));
		//Filter filter = ff.equals(ff.property("name"), ff.literal(name));
		
        WFSLog wfsLogProcess = new WFSLog(geoServer);
         
        features = wfsLogProcess.execute(features, DEFAULT_TYPE_NAME, wsName, storeName, filter, true, new NullProgressListener());
        
        if (features == null || features.isEmpty())
		{
    		if (progressListener != null)
    		{
    			progressListener.exceptionOccurred(new Exception("There was an error while logging FeatureType into the storage."));
    		}
			throw new ProcessException("There was an error while logging FeatureType into the storage.");
		}
        
		if (progressListener != null)
		{
			progressListener.progress(30);
		}

		/**
		 * RUN RasterAlgebra Process
		 */
		GridCoverage2D coverage = null;
		File f = null;
		GeoTiffWriter gtiffWriter = null;
		
		try
		{
			RasterAlgebraProcess rstAlgebraProcess = new RasterAlgebraProcess(catalog);
			coverage = rstAlgebraProcess.execute(attributeFilter, areaOfInterest, null);
			
			if (coverage == null)
			{
				throw new Exception("WPS RasterAlgebraProcess failed.");
			}
			
			final File directory = catalog.getResourceLoader().findOrCreateDirectory("data", wsName, storeName);
			f = new File(directory, name + ".tiff");
			gtiffWriter = new GeoTiffWriter(f);
			
			final ParameterValue<GeoToolsWriteParams> gtWparam = AbstractGridFormat.GEOTOOLS_WRITE_PARAMS.createValue();
			GeoTiffWriteParams param = new GeoTiffWriteParams();
			gtWparam.setValue(param);
			GeneralParameterValue[] params = new GeneralParameterValue[] { gtWparam };
			
			gtiffWriter.write(coverage, params);
		} 
		catch (Exception e)
		{
        	/**
    		 * Update Feature Attributes and LOG into the DB
    		 */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);
            
            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "FAILED");
            feature.setAttribute("itemStatusMessage", "There was an error while while processing Input parameters: "+e.getMessage());
            
            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);
    		
    		features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());

    		if (progressListener != null)
    		{
    			progressListener.exceptionOccurred(e);
    		}
			throw new ProcessException("There was an error while while processing Input parameters.", e);
		}
		finally
		{
			if (coverage != null)
			{
				try
				{
					coverage.dispose(true);
				}
				catch (Exception e) {
					// ignore
				}
			}

			if (gtiffWriter != null)
			{
				try
				{
					gtiffWriter.dispose();
				}
				catch (Exception e) {
					// ignore
				}
			}

		}

		GeoTiffReader gtiffReader = null;
		try
		{
			gtiffReader = new GeoTiffReader(f);
			coverage = gtiffReader.read(null);
		}
		catch (Exception e)
		{
        	/**
    		 * Update Feature Attributes and LOG into the DB
    		 */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);
            
            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "FAILED");
            feature.setAttribute("itemStatusMessage", "There was an error while while processing Input parameters: "+e.getMessage());
            
            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);
    		
    		features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());

    		if (progressListener != null)
    		{
    			progressListener.exceptionOccurred(e);
    		}
			throw new ProcessException("There was an error while while processing Input parameters.", e);
		}
		finally
		{
			if (gtiffWriter != null)
			{
				try
				{
					gtiffWriter.dispose();
				}
				catch (Exception e) {
					// ignore
				}
			}
		}
		
		final String rstAlgebraRasterStoreName = FilenameUtils.getBaseName(f.getAbsolutePath());
		final String rstAlgebraRasterLayerName = FilenameUtils.getBaseName(f.getAbsolutePath());

		String result = null;
		try
		{
			/**
			 * Import output Octave ASC layer into GeoServer
			 */
			ImportProcess importProcess = new ImportProcess(catalog);
			result = importProcess.execute(
					null,
					coverage,
					wsName,
					null,
					rstAlgebraRasterLayerName,
					crs,
					null,
					(styleName!=null?styleName:"raster"));
		}
		catch (Exception e)
		{
			/**
    		 * Update Feature Attributes and LOG into the DB
    		 */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);
            
            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "FAILED");
            feature.setAttribute("itemStatusMessage", e.getLocalizedMessage());
            
            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);
    		
    		features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());

    		if (progressListener != null)
    		{
    			progressListener.exceptionOccurred(e);
    		}
			throw new ProcessException(e);
		}
		
		if (result == null || !result.contains(rstAlgebraRasterLayerName))
		{
        	/**
    		 * Update Feature Attributes and LOG into the DB
    		 */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);
            
            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "FAILED");
            feature.setAttribute("itemStatusMessage", "There was an error while importing ASC layer into GeoServer.");
            
            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);
    		
    		features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());

    		if (progressListener != null)
    		{
    			progressListener.exceptionOccurred(new Exception("There was an error while importing ASC layer into GeoServer."));
    		}
			throw new ProcessException("There was an error while importing ASC layer into GeoServer.");
		}
        
		if (progressListener != null)
		{
			progressListener.progress(90);
		}
		
        if (features == null || features.isEmpty())
		{
        	/**
    		 * Update Feature Attributes and LOG into the DB
    		 */
            filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

            SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);
            
            // build the feature
            feature.setAttribute("runEnd", new Date());
            feature.setAttribute("itemStatus", "FAILED");
            feature.setAttribute("itemStatusMessage", "There was an error while logging FeatureType into the storage.");
            
            ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
            output.add(feature);
    		
    		features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());
    		
    		if (progressListener != null)
    		{
    			progressListener.exceptionOccurred(new Exception("There was an error while logging FeatureType into the storage."));
    		}
			throw new ProcessException("There was an error while logging FeatureType into the storage.");
		}
        
		if (progressListener != null)
		{
			/**
			 * Update Feature Attributes and LOG into the DB
			 */
	        filter = ff.equals(ff.property("ftUUID"), ff.literal(uuid.toString()));

	        SimpleFeature feature = SimpleFeatureBuilder.copy(features.subCollection(filter).toArray(new SimpleFeature[1])[0]);
	        
	        // build the feature
	        feature.setAttribute("storeName", rstAlgebraRasterStoreName);
	        feature.setAttribute("layerName", rstAlgebraRasterLayerName);
	        feature.setAttribute("runEnd", new Date());
	        feature.setAttribute("itemStatus", "COMPLETED");
	        feature.setAttribute("srcPath", f.getAbsolutePath());
	        
	        // set layer attribution info
	        LayerInfo layer = catalog.getLayerByName(new NameImpl(wsName, rstAlgebraRasterLayerName));
	        if (layer != null)
	        {
	        	AttributionInfo attribution = catalog.getFactory().createAttribution();
	        	attribution.setTitle(classification);
				layer.setAttribution(attribution);
				
				catalog.save(layer);
	        }
	        
	        ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
	        output.add(feature);
			
			features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());
			
			progressListener.progress(100);
			progressListener.complete();
		}

		
		return features;
	}

}