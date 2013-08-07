/* Copyright (c) 2012 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.wfs;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.config.GeoServer;
import org.geoserver.wps.gs.IDARasterAlgebraProcess;
import org.geoserver.wps.gs.IDASoundPropagationModelProcess;
import org.geotools.data.DataUtilities;
import org.geotools.util.logging.Logging;
import org.opengis.feature.simple.SimpleFeature;

public class IDADeleteTransactionListener implements TransactionListener {

	protected static final Logger LOGGER = Logging.getLogger(IDADeleteTransactionListener.class);

	protected GeoServer geoServer;

	protected Catalog catalog;

	List events = new ArrayList();
	List features = new ArrayList();

	public IDADeleteTransactionListener(GeoServer geoServer) {
		this.geoServer = geoServer;
		this.catalog = geoServer.getCatalog();
	}

	public void clear() {
		events.clear();
		features.clear();
	}

	public void dataStoreChange(TransactionEvent event) throws WFSException {
		events.add(event);
		String typeName = event.getAffectedFeatures().getSchema().getTypeName();
		
		if (LOGGER.isLoggable(Level.FINE))
		{
			LOGGER.fine("IDATxLIstener - catched request for FeatureType: " + typeName);
		}
		
		// check the correct event type and the correct FeatureType Name
		if ((typeName.contains(IDARasterAlgebraProcess.DEFAULT_TYPE_NAME) || typeName.contains(IDASoundPropagationModelProcess.DEFAULT_TYPE_NAME)) && TransactionEventType.PRE_DELETE == event.getType()) {
			features.addAll(DataUtilities.list(event.getAffectedFeatures()));
			for (Object ft : features) {
				if (ft instanceof SimpleFeature) {
					WorkspaceInfo ws = null;
					StoreInfo storeInfo = null;
					try {
						// retrieve workspace and store as inserted in the feature attributes
						SimpleFeature next = (SimpleFeature) ft;
						
						if (LOGGER.isLoggable(Level.FINE))
						{
							LOGGER.fine("IDATxLIstener - checking feature: " + next.getID());
						}

						String wsName = (String) next.getAttribute("wsName");
						String storeName = (String) next.getAttribute("storeName");
						String itemStatus = (String) next.getAttribute("itemStatus");
						String layerName = (String) next.getAttribute("layerName");

						if (LOGGER.isLoggable(Level.FINE))
						{
							LOGGER.fine("IDATxLIstener - wsName: " + wsName);
							LOGGER.fine("IDATxLIstener - storeName: " + storeName);
							LOGGER.fine("IDATxLIstener - layerName: " + layerName);
							LOGGER.fine("IDATxLIstener - itemStatus: " + itemStatus);
						}

						if (itemStatus != null && itemStatus.equals("RUNNING"))
						{
							LOGGER.severe("Exception occurred during Deletion: Could not remove a feature in status 'RUNNING'");
							//throw new WFSException("Exception occurred during Deletion: Could not remove a feature in status 'RUNNING'");
							continue;
						}
						
						if (wsName != null && storeName != null && itemStatus != null
								&& itemStatus.equals("COMPLETED")
								&& wsName.length() > 0
								&& storeName.length() > 0) {
							// being sure the workspace exists in the catalog
							ws = catalog.getWorkspaceByName(wsName);
							if (ws == null) {
								LOGGER.severe("Could not retrive WorkSpace "
										+ wsName + " into the Catalog");
							}

							if (ws != null) {
								// being sure the store exists in the catalog
								storeInfo = catalog.getDataStoreByName(ws.getName(), storeName.trim());
								storeInfo = (storeInfo == null ? catalog.getCoverageStoreByName(ws.getName(), storeName.trim()) : storeInfo);

								if (storeInfo == null) {
									LOGGER.severe("Could not retrive DataStore "
											+ storeName + " into the Catalog");
								}
								else
								{
									// drill down into layers (into resources since we cannot scan layers)
									int layersInStore = 0;
									try
									{
								        List<ResourceInfo> resources = catalog.getResourcesByStore(storeInfo, ResourceInfo.class);
								        for (ResourceInfo ri : resources) {
								            List<LayerInfo> layers = catalog.getLayers(ri);
								            if (!layers.isEmpty()){ 
								                for (LayerInfo li : layers) {
								                	// counting the store layers, if 0 we can remove the whole store too ...
								                	layersInStore++;
								                	// we need to check the layer name start, since for the coverages a timestamp is attached to the name
							                		
								                	if (LOGGER.isLoggable(Level.FINE))
													{
														LOGGER.fine("IDATxLIstener - going to check layer: " + li.getName() + " against feture layer attribute: " + layerName);
													}
	
							                		if (layerName != null && li != null && li.getName() != null && li.getName().equals(layerName))
								                	{
								                		catalog.remove(li);
								                		catalog.remove(li.getResource());
								                		layersInStore--;
	
								                		if (LOGGER.isLoggable(Level.FINE))
														{
															LOGGER.fine("IDATxLIstener - removed layer: " + li.getName());
														}
								                	}
								                }
								            }
								        }
								        
								        // the store does not contain layers anymore, lets remove it from the catalog then
								        
								        if (LOGGER.isLoggable(Level.FINE))
										{
											LOGGER.fine("IDATxLIstener - remaining layers for this store: " + layersInStore);
										}
								        
								        if (layersInStore == 0 && storeInfo.getName().equals(storeName))
								        {
								        	//geoServer.reload();
								        	catalog.remove(storeInfo);
	
									        if (LOGGER.isLoggable(Level.FINE))
											{
												LOGGER.fine("IDATxLIstener - removed store: " + storeInfo.getName());
											}
								        }
									}
									catch (Exception e)
									{
										LOGGER.severe("Could not remove store and layer from catalog: " + e.getLocalizedMessage());
										
									}
								}
							}
						}

						String srcPath = (String) next.getAttribute("srcPath");
						
				        if (LOGGER.isLoggable(Level.FINE))
						{
							LOGGER.fine("IDATxLIstener - going to delete file: " + srcPath);
						}

						if (ws != null && storeInfo != null && srcPath != null && srcPath.length() > 0) {
							File file = new File(srcPath);

							// try to delete the file several times since it may be locked by catalog for some reason
							if (file.exists() && file.isFile() && file.canWrite()) {
								final int retries = 10;
								int tryDelete = 0;
								for (; tryDelete < 5; tryDelete++)
								{
									if (file.delete())
									{
								        if (LOGGER.isLoggable(Level.FINE))
										{
											LOGGER.fine("IDATxLIstener - deleted file: " + srcPath);
										}

								        break;
									}
									else
									{
								        if (LOGGER.isLoggable(Level.FINE))
										{
											LOGGER.fine("IDATxLIstener - could not delete file: " + srcPath + " ... wait 5 secs and retry...");
										}

								        // wait 5 seconds and try again...
										Thread.sleep(5000);
									}
								}
								
								if (tryDelete > retries)
								{
									LOGGER.severe("Could not delete file "+srcPath+" from the FileSystem");
								}
							}
						}
					} catch (Exception e) {
						LOGGER.severe("Exception occurred during Deletion: "+e.getLocalizedMessage());
						throw new WFSException(e);
					} finally {
					}
				}
			}
		}
	}
}