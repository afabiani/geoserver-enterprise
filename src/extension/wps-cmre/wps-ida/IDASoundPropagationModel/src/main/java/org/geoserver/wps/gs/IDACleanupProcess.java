package org.geoserver.wps.gs;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.geoserver.catalog.Catalog;
import org.geoserver.config.GeoServer;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.process.gs.GSProcess;
import org.geotools.util.logging.Logging;
import org.opengis.util.ProgressListener;
import org.vfny.geoserver.global.GeoserverDataDirectory;

@DescribeProcess(title = "IDA Cleanup Process", description = "This process can be invoked to purge old IDA temp files.")
public class IDACleanupProcess implements GSProcess {

	protected static final Logger LOGGER = Logging.getLogger(IDACleanupProcess.class);
	
	protected GeoServer geoServer;
	
	protected Catalog catalog;
	
	public IDACleanupProcess(GeoServer geoServer) {
		this.geoServer = geoServer;
		this.catalog = geoServer.getCatalog();
	}
	
	@DescribeResult(name = "result", description = "Cleanup completed successfully or not.")
	public Boolean execute(ProgressListener progressListener) throws ProcessException {
		try
		{
			Properties idaExecProperties = new Properties();
			//idaExecProperties.load(IDASoundPropagationModelProcess.class.getClassLoader().getResourceAsStream("ida-exec.properties"));
			idaExecProperties.load(new FileInputStream(GeoserverDataDirectory.findConfigFile("ida-exec.properties").getAbsolutePath()));
			
			// Cleaning up uploaded profiles
			if(idaExecProperties.containsKey("input.profiles.upload.folder")) {
				File profilesUploadFolderPath = new File((String) idaExecProperties.get("input.profiles.upload.folder"));
				FileUtils.cleanDirectory(profilesUploadFolderPath);
				
				LOGGER.info("IDACleanupProcess - cleaned up 'input.profiles.upload.folder': " + profilesUploadFolderPath.getAbsolutePath());
			}
			if(idaExecProperties.containsKey("input.profiles.folder")) {
				File profilesFolderPath = new File((String) idaExecProperties.get("input.profiles.folder"));
				FileUtils.cleanDirectory(profilesFolderPath);
				
				LOGGER.info("IDACleanupProcess - cleaned up 'input.profiles.folder': " + profilesFolderPath.getAbsolutePath());
			}
			
			// Cleaning up Octave generated ascii files
			if(idaExecProperties.containsKey("output.path")) {
				File octaveOutputFolderPath = new File((String) idaExecProperties.get("output.path"));
				FileUtils.cleanDirectory(octaveOutputFolderPath);
				
				LOGGER.info("IDACleanupProcess - cleaned up 'output.path': " + octaveOutputFolderPath.getAbsolutePath());
			}
			
		} 
		catch (Exception e)
		{
			LOGGER.severe("IDACleanupProcess - exception occurred while cleaning up temp folders: " + e.getLocalizedMessage());
			return false;
		}
		
		return true;
	}

}