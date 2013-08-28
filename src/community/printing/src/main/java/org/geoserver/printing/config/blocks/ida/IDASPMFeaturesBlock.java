/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */

/*
 * Copyright (C) 2009  Camptocamp
 *
 * This file is part of MapFish Server
 *
 * MapFish Server is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MapFish Server is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with MapFish Server.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.geoserver.printing.config.blocks.ida;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.geoserver.printing.config.blocks.FeaturesBlock;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.mapfish.print.InvalidValueException;
import org.mapfish.print.PDFUtils;
import org.mapfish.print.RenderingContext;
import org.mapfish.print.utils.PJsonObject;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.lowagie.text.DocumentException;
import com.lowagie.text.Font;
import com.lowagie.text.Paragraph;
import com.lowagie.text.pdf.PdfPCell;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Bean to configure a !legends block.
 * <p/>
 * See http://trac.mapfish.org/trac/mapfish/wiki/PrintModuleServer#Legendsblock
 */
public class IDASPMFeaturesBlock extends FeaturesBlock {
    public static final Logger LOGGER = Logger.getLogger(IDASPMFeaturesBlock.class);

    static final String ECKERT_IV_WKT = "PROJCS[\"World_Eckert_IV\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Eckert_IV\"],PARAMETER[\"Central_Meridian\",0.0],UNIT[\"Meter\",1.0]]";

    protected PdfPCell createLine(RenderingContext context, double indent, PJsonObject node, Font layerPdfFont, Font classPdfFont, PJsonObject params) throws DocumentException {
        final PJsonObject properties = node.optJSONObject("properties");

        final Paragraph result = new Paragraph();
        if (properties != null) {
        	String name = "";
        	Map<String, Double> aoi = new HashMap<String, Double>();
        	Map<String, String> map = new HashMap<String, String>();
        	Map<String, String[]> coveragesMap = new HashMap<String, String[]>();
        	final Iterator<String> keyIterator = properties.keys(); 
            while (keyIterator.hasNext()) {
            	final String key = keyIterator.next();
            	final String value = properties.getString(key).replace("\\", "/");
            	
            	if (key.equals("coverages") || key.equals("avg") || key.equals("count") || key.equals("min") || key.equals("max") || key.equals("sum") || key.equals("stddev")) {
            		coveragesMap.put(key, value.split(" | "));
            		continue;
            	}

            	if (key.equalsIgnoreCase("areaOfInterest")) {
            		Double area = 0.0;
            		
            		try {
            			CoordinateReferenceSystem crs = CRS.decode("EPSG:4326", true);
            			CoordinateReferenceSystem targetCRS = CRS.parseWKT(ECKERT_IV_WKT);
            			MathTransform firstTransform = CRS.findMathTransform(crs, targetCRS);
            			
            			String[] points = value.substring(1, value.length()-1).trim().split(";");
            			
            			double x1 = Double.parseDouble(points[0].trim().split(" ")[0].trim());
						double x2 = Double.parseDouble(points[1].trim().split(" ")[0].trim());
						double y1 = Double.parseDouble(points[0].trim().split(" ")[1].trim());
						double y2 = Double.parseDouble(points[1].trim().split(" ")[1].trim());
						Envelope env = new Envelope(x1, x2, y1, y2);
            			Polygon sourceGeometry = JTS.toGeometry(env);
						
						// reproject densifiedGeometry to Eckert IV
			            Geometry targetGeometry = JTS.transform(sourceGeometry, firstTransform);
			            area = targetGeometry.getArea()/(Math.pow(10, 6));
            		} catch (Exception e) {
            			LOGGER.warn("Exception occurred while computing gometry Area!", e);
                    }
                    
					aoi.put(value, area);
					continue;
            	}
            	
            	if (key.equalsIgnoreCase("modelName") || key.equalsIgnoreCase("attributeName")) {
            		name = value;
            		continue;
            	}
            	
            	if (!key.toLowerCase().contains("id") && !key.startsWith("item") && !key.startsWith("srcPath") && !key.contains("Name") && !key.equalsIgnoreCase("outputUrl")) {
            		map.put(key, value);
            	}

            }
            
            result.setFont(layerPdfFont);
            result.add(name + "\r\n----------------------------------------------------\r\n");
            
        	SortedSet<String> keys = new TreeSet<String>(map.keySet());
        	for (String key : keys) {
            	if (properties.has(key)) {
                    String property = map.get(key);
                    try {
                    	result.setFont(layerPdfFont);
                        result.add(PDFUtils.renderString(context, params, key + ":  ", layerPdfFont));
                        result.setFont(classPdfFont);
                        result.add(PDFUtils.renderString(context, params, property, classPdfFont));
                        result.add(" \r\n");
                    } catch (InvalidValueException e) {
                        LOGGER.warn("Failed to create image chunk: " + e.getMessage());
                    }
            	}
            }
        	
        	if (aoi != null && !aoi.isEmpty()) {
				result.setFont(layerPdfFont);
				result.add("Area of interest \r\n");
				result.setFont(classPdfFont);
				for (String aoiKey : aoi.keySet()) {
					result.add("  - Extension : " + aoiKey + " \r\n");
					result.add("  - Area : " + aoi.get(aoiKey) + " Km2 \r\n");
				}
        	}
        	
        	if (coveragesMap != null && !coveragesMap.isEmpty()) {
        		int i = 0;
        		for (String coverage : coveragesMap.get("coverages")) {
        			if (!coverage.contains("|")) {
        				result.setFont(layerPdfFont);
        				result.add("Covergae : " + coverage + " \r\n");
        				result.setFont(classPdfFont);
        				result.add("  - Count : " + coveragesMap.get("count")[i] + " \r\n");
        				result.add("  - Sum : " + coveragesMap.get("sum")[i] + " \r\n");
        				result.add("  - Min : " + coveragesMap.get("min")[i] + " \r\n");
        				result.add("  - Max : " + coveragesMap.get("max")[i] + " \r\n");
        				result.add("  - Avg : " + coveragesMap.get("avg")[i] + " \r\n");
        				result.add("  - Std Dev : " + coveragesMap.get("stddev")[i] + " \r\n");
        			}
        			
        			i++;
        		}
        		
        	}
            result.add(" \r\n");
        }

        final PdfPCell cell = new PdfPCell(result);
        cell.setBorder(PdfPCell.NO_BORDER);
        cell.setPadding(1f);
        cell.setPaddingLeft((float) indent);

        if (getBackgroundColorVal(context, params) != null) {
            cell.setBackgroundColor(getBackgroundColorVal(context, params));
        }

        return cell;
    }
}