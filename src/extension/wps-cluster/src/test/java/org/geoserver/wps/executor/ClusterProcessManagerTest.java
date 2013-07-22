/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.geoserver.ows.Ows11Util;
import org.geoserver.wps.WPSTestSupport;
import org.geoserver.wps.executor.DefaultProcessManager.ProcessListener;
import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.opengis.feature.type.Name;

import com.thoughtworks.xstream.XStream;

/**
 * Alternative implementation of ProcessManager, using a storage (ProcessStorage) to share process status between the instances of a cluster.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 * 
 */
public class ClusterProcessManagerTest extends WPSTestSupport {

    public void testSerialization() throws Exception {

        ExecutionStatusExTest statusSrc = new ExecutionStatusExTest(Ows11Util.name("test_process"),
                "0");

        statusSrc.setProgress(99.9f);
        statusSrc.setPhase(ProcessState.RUNNING);

        Map<String, Object> testOutput = new HashMap<String, Object>();
        testOutput.put("id1", new Integer(1));
        testOutput.put("id2", new String("2"));
        testOutput.put("id3", new Double(3.0));
        MyClass myClass = new MyClass();
        myClass.setValue(new BigDecimal(4));
        testOutput.put("id4", myClass);

        statusSrc.setOutput(testOutput);

        XStream xstream = new XStream();

        String marshalled = xstream.toXML(statusSrc);

        ExecutionStatus statusTrg = (ExecutionStatus) xstream.fromXML(marshalled);

        assertEquals(statusTrg.getExecutionId(), statusSrc.getExecutionId());
        assertEquals(statusTrg.getProcessName().getLocalPart(), statusSrc.getProcessName()
                .getLocalPart());

        assertEquals(statusTrg.getProgress(), statusSrc.getProgress());
        assertEquals(statusTrg.getPhase(), statusSrc.getPhase());

        Map<String, Object> trgOutput = statusTrg.getOutput(0);

        for (Entry<String, Object> entry : trgOutput.entrySet()) {
            if (entry.getValue() instanceof MyClass) {
                assertEquals(((MyClass) entry.getValue()).getValue(),
                        ((MyClass) testOutput.get(entry.getKey())).getValue());
            } else {
                assertEquals(entry.getValue(), testOutput.get(entry.getKey()));
            }
        }
    }

    class MyClass {
        private BigDecimal value;

        public void setValue(BigDecimal value) {
            this.value = value;
        }

        public BigDecimal getValue() {
            return value;
        }
    }

    /**
     * A pimped up test execution status
     * 
     * @author Alessio Fabiani - GeoSolutions
     */
    static class ExecutionStatusExTest extends ExecutionStatus {

        private Map<String, Object> output;

        ProcessListener listener;

        public ExecutionStatusExTest(Name processName, String executionId) {
            super(processName, executionId, ProcessState.QUEUED, 0);
        }

        public ExecutionStatus getStatus() {
            return new ExecutionStatus(processName, executionId, phase, progress);
        }

        @Override
        public void setPhase(ProcessState phase) {
            super.setPhase(phase);

        }

        @Override
        public Map<String, Object> getOutput(long timeout) throws Exception {
            if (output == null)
                throw new Exception("Null output!");
            return output;
        }

        public void setOutput(Map<String, Object> output) {
            this.output = output;
        }

    }
}