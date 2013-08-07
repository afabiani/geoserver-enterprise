package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.ppio.FeatureAttribute;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.gce.arcgrid.ArcGridFormat;
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
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.util.ProgressListener;

import com.vividsolutions.jts.geom.Point;

import freemarker.template.Configuration;
import freemarker.template.Template;

@DescribeProcess(title = "IDA Sound Propagation Model", description = "SPM Workflow. Gets SPM input params, logs to the DB and executes Matlab code.")
public class IDASoundPropagationModelProcess implements GSProcess {

	protected static final Logger LOGGER = Logging.getLogger(IDASoundPropagationModelProcess.class);
	
	protected GeoServer geoServer;
	
	protected Catalog catalog;
	
	protected FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
	
	protected GeometryBuilder geomBuilder = new GeometryBuilder();

	public static final String DEFAULT_TYPE_NAME = "IDASoundPropModel";

	public IDASoundPropagationModelProcess(GeoServer geoServer) {
		this.geoServer = geoServer;
		this.catalog = geoServer.getCatalog();
	}
	
	@DescribeResult(name = "result", description = "List of attributes to be converted to a FeatureType")
	public SimpleFeatureCollection execute(
			@DescribeParameter(name = "userId", min = 1, description = "SPM attribute userId") String userId,
			@DescribeParameter(name = "modelName", min = 1, description = "SPM attribute modelName") String name,
			@DescribeParameter(name = "outputUrl", min = 1, description = "SPM attribute outputUrl") URL outputUrl,
			@DescribeParameter(name = "runBegin", min = 1, description = "SPM attribute runBegin") Date runBegin,
			@DescribeParameter(name = "runEnd", min = 0, description = "SPM attribute runEnd") Date runEnd,
			@DescribeParameter(name = "itemStatus", min = 0, description = "SPM attribute item_status") String itemStatus,
			@DescribeParameter(name = "itemStatusMessage", min = 0, description = "SPM attribute itemStatusMessage") String itemStatusMessage,
			@DescribeParameter(name = "wsName", min = 1, description = "SPM attribute workspaceName") String wsName,
			@DescribeParameter(name = "storeName", min = 0, description = "SPM attribute storeName") String storeName,
			@DescribeParameter(name = "layerName", min = 0, description = "SPM attribute layerName") String layerName,
			@DescribeParameter(name = "styleName", min = 0, description = "SPM attribute styleName") String styleName,
			@DescribeParameter(name = "srcPath", min = 0, description = "SPM attribute src_path") String srcPath,
			@DescribeParameter(name = "season", description = "SPM attribute season") String season,
			@DescribeParameter(name = "sourceDepth", description = "SPM attribute sourceDepth") Double sourceDepth,
			@DescribeParameter(name = "sourceFrequency", description = "SPM attribute sourceFrequency") Double sourceFrequency,
			@DescribeParameter(name = "sourcePressureLevel", description = "SPM attribute src_pressure_level") Double sourcePressureLevel,
			@DescribeParameter(name = "soundVelocityProfile", min = 0, description = "SPM attribute soundVelocityProfile") String soundVelocityProfile,
			@DescribeParameter(name = "svpSourceUnit", min = 0, description = "SPM attribute soundVelocityProfile location") Point svpSourceUnit,
			@DescribeParameter(name = "tlModel", min = 0, description = "SPM attribute tlModel") String tlModel,
			@DescribeParameter(name = "bottomType", min = 0, description = "SPM attribute bottomType") String bottomType,
			@DescribeParameter(name = "quality", min = 0, description = "SPM attribute quality") String quality,
			@DescribeParameter(name = "maxRange", min = 0, description = "SPM attribute maxRange") Double maxRange,
			@DescribeParameter(name = "advParams", min = 0, description = "SPM attribute advancedParams (par1;par2)") String advParams,
			@DescribeParameter(name = "soundSourceUnit", description = "SPM attribute footprint") Point soundSourceUnit,
			ProgressListener progressListener) throws ProcessException {

		List<String> advancedParams = null;
		if (advParams != null && advParams.length()>0)
		{
			advancedParams = Arrays.asList(advParams.split(";"));
		}
		
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
			if (soundSourceUnit.getSRID() <= 0) {
				crs = CRS.decode("EPSG:4326");
				soundSourceUnit.setSRID(4326);
			} else {
				crs = CRS.decode("EPSG:" + soundSourceUnit.getSRID());
			}
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
		attributes.add(new FeatureAttribute("userId", userId));
		attributes.add(new FeatureAttribute("modelName", name));
		attributes.add(new FeatureAttribute("outputUrl", outputUrl.toExternalForm()));
		attributes.add(new FeatureAttribute("runBegin", runBegin));
		attributes.add(new FeatureAttribute("runEnd", (runEnd!=null?runEnd:new Date())));
		attributes.add(new FeatureAttribute("itemStatus", (itemStatus != null ? itemStatus : "RUNNING")));
		attributes.add(new FeatureAttribute("itemStatusMessage", (itemStatusMessage != null ? itemStatusMessage : "Instrumented by Server")));
		attributes.add(new FeatureAttribute("wsName", wsName));
		attributes.add(new FeatureAttribute("storeName", (storeName != null ? storeName : "")));
		attributes.add(new FeatureAttribute("layerName", (layerName != null ? layerName : "")));
		attributes.add(new FeatureAttribute("srcPath", (srcPath != null ? srcPath : "")));
		attributes.add(new FeatureAttribute("season", (season != null ? season : "")));
		attributes.add(new FeatureAttribute("sourceDepth", (sourceDepth != null ? sourceDepth : 0.0)));
		attributes.add(new FeatureAttribute("sourceFrequency", (sourceFrequency != null ? sourceFrequency : 0.0)));
		attributes.add(new FeatureAttribute("sourcePressureLevel", (sourcePressureLevel != null ? sourcePressureLevel : 0.0)));
		attributes.add(new FeatureAttribute("soundVelocityProfile", (soundVelocityProfile != null ? soundVelocityProfile : "")));
		attributes.add(new FeatureAttribute("svpSourceUnitX", (svpSourceUnit != null ? svpSourceUnit.getX() : 0.0)));
		attributes.add(new FeatureAttribute("svpSourceUnitY", (svpSourceUnit != null ? svpSourceUnit.getY() : 0.0)));
		attributes.add(new FeatureAttribute("tlModel", (tlModel != null ? tlModel : "")));
		attributes.add(new FeatureAttribute("bottomType", (bottomType != null ? bottomType : "")));
		attributes.add(new FeatureAttribute("quality", (quality != null ? quality : "")));
		attributes.add(new FeatureAttribute("maxRange", (maxRange != null ? maxRange : 0.0)));

		SimpleFeatureCollection features = toFeatureProcess.execute(soundSourceUnit, crs, DEFAULT_TYPE_NAME, attributes, null);
		
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
		 * RUN Octave SPM process
		 */
		GridCoverage2D coverage = null;
		File f = null;
		
		try
		{
			Properties idaExecProperties = new Properties();
			idaExecProperties.load(IDASoundPropagationModelProcess.class.getClassLoader().getResourceAsStream("ida-exec.properties"));
			
			List<String> cmdOptionPath = new LinkedList<String>();
			
			String executablePath = idaExecProperties.getProperty("executable.command");
			cmdOptionPath.add(executablePath);
			
			for(Entry<Object, Object> entry : idaExecProperties.entrySet())
			{
				if (entry.getKey().toString().startsWith("option."))
				{
					String optionName = entry.getKey().toString().substring("option.".length());
					String optionValue = (String) entry.getValue();
					
					if (optionName!=null && !optionName.trim().isEmpty())
					{
						cmdOptionPath.add((optionName.trim().length()==1?"-":"--") + optionName.trim());
					}

					if (optionValue!=null && !optionValue.trim().isEmpty())
					{
						cmdOptionPath.add(optionValue.trim());
					}
				}
			}

			//Freemarker configuration object

			//final File directory = catalog.getResourceLoader().findOrCreateDirectory("data", wsName, storeName);
			final File directory = new File(idaExecProperties.getProperty("output.path"));
			f = new File(directory, name + "_");

	        File templateFile = new File(idaExecProperties.getProperty("input.path") + "/" + idaExecProperties.getProperty("input.template"));
	        if (!templateFile.exists() || !templateFile.isFile() || !templateFile.canRead())
	        {
	        	throw new IOException("Input template file is not accessible or cannot be read.");
	        }

	        Configuration cfg = new Configuration();
	        cfg.setDirectoryForTemplateLoading(new File(idaExecProperties.getProperty("input.path")));
	        cfg.setLocalizedLookup(false);
	        
            Template template = cfg.getTemplate(idaExecProperties.getProperty("input.template"));
            
            // Build the data-model
            Map<String, Object> data = new HashMap<String, Object>();
            NumberFormat nff = new DecimalFormat("#0.00000#");
            ((DecimalFormat)nff).setDecimalFormatSymbols(DecimalFormatSymbols.getInstance(Locale.US));
            data.put("soundSourceUnitX", nff.format(soundSourceUnit.getX()));
            data.put("soundSourceUnitY", nff.format(soundSourceUnit.getY()));
            data.put("season", season);
            if(soundVelocityProfile!=null && !soundVelocityProfile.isEmpty())
            {
            	File sndProfilePath = new File(idaExecProperties.getProperty("input.profiles.upload.folder"), soundVelocityProfile);
            	
            	if(sndProfilePath.exists() && sndProfilePath.isFile() && !sndProfilePath.isDirectory() && sndProfilePath.canWrite())
            	{
            		File sndProfDest = new File(idaExecProperties.getProperty("input.profiles.folder"), FilenameUtils.getBaseName(soundVelocityProfile));
					if(sndProfilePath.renameTo(sndProfDest))
					{
						data.put("soundVelocityProfile", sndProfDest.getAbsolutePath());
					}
					else
					{
						data.put("soundVelocityProfile", "<-- ERROR occurred while processing the input sound velocity profile file -->");
					}
            	}
            	else
            	{
            		data.put("soundVelocityProfile", "<-- ERROR occurred while processing the input sound velocity profile file -->");
            	}
            }
            else
            {
            	data.put("soundVelocityProfile", "");
            }
            
            if (svpSourceUnit != null)
            {
                data.put("svpSourceUnitX", nff.format(svpSourceUnit.getX()));
                data.put("svpSourceUnitY", nff.format(svpSourceUnit.getY()));            	
            } else {
                data.put("svpSourceUnitX", nff.format(0.0));
                data.put("svpSourceUnitY", nff.format(0.0));            	
            }

            data.put("tlModel", sourceDepth);
            data.put("bottomType", sourceDepth);
            data.put("quality", sourceDepth);

            if (maxRange != null)
            {
            	data.put("maxRange", nff.format(maxRange));
            }
            else
            {
            	data.put("maxRange", maxRange);
            }
            
            data.put("sourceDepth", sourceDepth);
            data.put("sourceFrequency", sourceFrequency);
            data.put("sourcePressureLevel", sourcePressureLevel);
            data.put("modelName", name);
            data.put("advancedParams", advancedParams);
            data.put("outputPath", f.getAbsolutePath().replaceAll("\\\\", "/"));
            
            // File output
            File inputFile = null;
            Writer file = null;
			try {
				inputFile = File.createTempFile("idaExecInputTemplate_" + System.nanoTime(), ".txt");
				file = new FileWriter (inputFile);
				template.process(data, file);
				
			} catch (Exception e) {
				throw new Exception(e);
			}
			finally
			{
				if (file!=null){
					file.flush();
				}
				if (file!=null){
					file.close();
				}
			}
            
            cmdOptionPath.add(inputFile.getAbsolutePath());
    		
			CmdLine cmdLineProcess = new CmdLine(geoServer);

			// --silent --eval \"rxlevel({_SPL},{_LAT},{_LON},{_FILEPATH});\" --path C:/Programmi/MMRM-TDA/matlabcode2/
	        // --verbose --eval \"rxlevel(1,40,6,\\\"C:/data/NURC-IDA/output/test_\\\");\" --path C:/data/NURC-IDA/matlabcode2/
			LOGGER.info("Executing Octave command with parameters: " + cmdOptionPath);

			/*String results = cmdLineProcess.execute(
	        		Arrays.asList(
	        				executablePath, 
	        				//"--silent", //"",
	        				"--eval", "rxlevel("+(srcPressureLevel==0||Double.isNaN(srcPressureLevel)?1:srcPressureLevel)+","+footprint.getY()+","+footprint.getX()+",\""+f.getAbsolutePath().replaceAll("\\\\", "/")+"\");", 
	        				"--path", octaveConfigFilePath,
	        				"--verbose"),
	        		directory,
	        		true,
	        		new NullProgressListener());*/

			String results = cmdLineProcess.execute(
					cmdOptionPath,
	        		directory,
	        		true,
	        		new NullProgressListener());

			LOGGER.info("Command Line Process results : " + results);
			
			f = new File(directory, idaExecProperties.getProperty("output.prefix") + name + "_" + idaExecProperties.getProperty("output.suffix"));
			
			LOGGER.info("Trying to read ASC file: " + f.getAbsolutePath());
			
	        // and then we try to read it as a asc
	        coverage = new ArcGridFormat().getReader(f).read(null);
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

		final String spmRasterStoreName = FilenameUtils.getBaseName(f.getAbsolutePath());
		final String spmRasterLayerName = FilenameUtils.getBaseName(f.getAbsolutePath());

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
					spmRasterLayerName,
					crs,
					null,
					(styleName!=null?styleName:"spm"));
			
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
		
		if (result == null || !result.contains(spmRasterLayerName))
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
	        feature.setAttribute("storeName", spmRasterStoreName);
	        feature.setAttribute("layerName", spmRasterLayerName);
	        feature.setAttribute("runEnd", new Date());
	        feature.setAttribute("itemStatus", "COMPLETED");
	        feature.setAttribute("srcPath", f.getAbsolutePath());
	        
	        ListFeatureCollection output = new ListFeatureCollection(features.getSchema());
	        output.add(feature);
			
			features = wfsLogProcess.execute(output, DEFAULT_TYPE_NAME, wsName, storeName, filter, false, new NullProgressListener());
			
			progressListener.progress(100);
			progressListener.complete();
		}

		
		return features;
	}

}