/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.ppio;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.geoserver.catalog.Catalog;
import org.geoserver.config.GeoServer;
import org.geoserver.data.util.IOUtils;
import org.geoserver.wps.resource.WPSResourceManager;

/**
 * Handles input and output of feature collections as zipped files.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class ZipArchivePPIO extends BinaryPPIO {

    /** The geo server. */
    GeoServer geoServer;

    /** The catalog. */
    Catalog catalog;

    /** The resources. */
    WPSResourceManager resources;

    /**
     * Instantiates a new zip archive ppio.
     *
     * @param geoServer the geo server
     * @param resources the resources
     */
    protected ZipArchivePPIO(GeoServer geoServer, WPSResourceManager resources) {
        super(File.class, File.class, "application/zip");
        this.geoServer = geoServer;
        this.catalog = geoServer.getCatalog();
        this.resources = resources;
    }

    /**
     * Encode.
     *
     * @param output the output
     * @param os the os
     * @throws Exception the exception
     */
    @Override
    public void encode(final Object output, OutputStream os) throws Exception {
        ZipOutputStream zipout = new ZipOutputStream(os);
        try {
            File directory = catalog.getResourceLoader().findOrCreateDirectory("downloads");
            FileUtils.copyFileToDirectory((File) output, directory);
            IOUtils.zipDirectory(directory, zipout, new FilenameFilter() {

                public boolean accept(File parent, String name) {
                    return name.equals(FilenameUtils.getName(((File) output).getName()));
                }
            });
            zipout.finish();
        } finally {
            FileUtils.deleteQuietly((File) output);
        }
    }

    /**
     * Decode.
     *
     * @param input the input
     * @return the object
     * @throws Exception the exception
     */
    @Override
    public Object decode(InputStream input) throws Exception {
        // create the temp directory and register it as a temporary resource
        final File directory = catalog.getResourceLoader().findOrCreateDirectory("downloads");

        // unzip to the temporary directory
        ZipInputStream zis = null;
        try {
            zis = new ZipInputStream(input);
            ZipEntry entry = null;

            while ((entry = zis.getNextEntry()) != null) {
                String name = entry.getName();
                File file = new File(directory, entry.getName());
                if (entry.isDirectory()) {
                    file.mkdir();
                } else {
                    int count;
                    byte data[] = new byte[4096];
                    // write the files to the disk
                    FileOutputStream fos = null;
                    try {
                        fos = new FileOutputStream(file);
                        while ((count = zis.read(data)) != -1) {
                            fos.write(data, 0, count);
                        }
                        fos.flush();
                    } finally {
                        if (fos != null) {
                            fos.close();
                        }
                    }
                }
                zis.closeEntry();
            }
        } finally {
            if (zis != null) {
                zis.close();
            }
        }

        return directory;
    }

    /**
     * Gets the file extension.
     *
     * @return the file extension
     */
    @Override
    public String getFileExtension() {
        return "zip";
    }

    /**
     * The Class ZipArchive.
     */
    public static class ZipArchive extends ZipArchivePPIO {

        /**
         * Instantiates a new zip archive.
         *
         * @param geoServer the geo server
         * @param resources the resources
         */
        public ZipArchive(GeoServer geoServer, WPSResourceManager resources) {
            super(geoServer, resources);
        }

    }
}
