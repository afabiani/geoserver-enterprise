/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.gs;

import java.io.StringWriter;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.output.WriterOutputStream;
import org.geoserver.data.test.MockData;
import org.geoserver.test.GeoServerTestSupport;
import org.geoserver.wps.executor.ExecutionStatus;
import org.geoserver.wps.ppio.ExecutionStatusListPPIO;
import org.geoserver.wps.ppio.ExecutionStatusListPPIO.ExecutionStatusList;
import org.geotools.util.NullProgressListener;

/**
 * The Class ClusterManagerProcessTest.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
public class ClusterManagerProcessTest extends GeoServerTestSupport {

    /**
     * Populate data directory.
     *
     * @param dataDirectory the data directory
     * @throws Exception the exception
     */
    @Override
    protected void populateDataDirectory(MockData dataDirectory) throws Exception {
        super.populateDataDirectory(dataDirectory);

        dataDirectory.addWcs10Coverages();
    }

    /**
     * PPIO Test.
     *
     * @throws Exception the exception
     */
    public void testEncodeStatus() throws Exception {
        ClusterManagerProcess managerProcess = new ClusterManagerProcess(getGeoServer());

        final String executionId = UUID.randomUUID().toString();
        List<ExecutionStatus> status = managerProcess.execute(executionId,
                new NullProgressListener() // progressListener
                );

        assertNotNull(status);
        assertEquals(1, status.size());

        ExecutionStatusList ppio = new ExecutionStatusListPPIO.ExecutionStatusList(getGeoServer(),
                null);
        StringWriter writer = new StringWriter();
        ppio.encode(status, new WriterOutputStream(writer));

        String statusList = writer.toString();

        assertNotNull(statusList);

        Object outputStatus = ppio.decode(statusList);

        try {
            List<ExecutionStatus> outputStatusList = (List<ExecutionStatus>) outputStatus;

            assertNotNull(outputStatusList);
            assertEquals(1, outputStatusList.size());

            assertEquals(status.get(0).getExecutionId(), outputStatusList.get(0).getExecutionId());
            assertEquals(status.get(0).getProcessName(), outputStatusList.get(0).getProcessName());
            assertEquals(status.get(0).getPhase(), outputStatusList.get(0).getPhase());
            assertEquals(status.get(0).getProgress(), outputStatusList.get(0).getProgress());
        } catch (Exception e) {
            assertFalse(true);
        }
    }
}
