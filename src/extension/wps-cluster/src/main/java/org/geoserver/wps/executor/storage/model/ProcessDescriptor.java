/* Copyright (c) 2001 - 2007 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, availible at the root
 * application directory.
 */
package org.geoserver.wps.executor.storage.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.geoserver.wps.executor.ExecutionStatus.ProcessState;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

/**
 * The Class ProcessDescriptor.
 * 
 * @author "Alessio Fabiani - alessio.fabiani@geo-solutions.it"
 */
@Entity(name = "ProcessDescriptor")
@Table(name = "gs_processdescriptor")
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE, region = "processdescriptor")
@XmlRootElement(name = "ProcessDescriptor")
@XmlType(propOrder = { "id", "clusterId", "executionId", "status", "phase", "progress", "result" })
public class ProcessDescriptor implements Identifiable, Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3654914559439623648L;

    /** The id. */
    @Id
    @GeneratedValue
    @Column
    private Long id;

    /** The cluster id. */
    @Column(nullable = false, updatable = true)
    private String clusterId;

    /** The execution id. */
    @Column(nullable = false, updatable = true)
    private String executionId;

    /** The phase. */
    @Enumerated(EnumType.ORDINAL)
    @Column(nullable = false, updatable = true)
    private ProcessState phase;

    /** The status. */
    @Lob
    @Column(nullable = false, updatable = true)
    private String status;

    /** The progress. */
    @Column(nullable = false, updatable = true)
    private float progress;

    /** The result. */
    @Lob
    @Column(nullable = true, updatable = true)
    private String result;

    /**
     * Instantiates a new instance.
     */
    public ProcessDescriptor() {

    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public Long getId() {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id the new id
     */
    public void setId(Long id) {
        this.id = id;
    }

    /**
     * Sets the cluster id.
     *
     * @param clusterId the new cluster id
     */
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    /**
     * Gets the cluster id.
     *
     * @return the cluster id
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Sets the execution id.
     *
     * @param executionId the new execution id
     */
    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    /**
     * Gets the execution id.
     *
     * @return the execution id
     */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * Sets the phase.
     *
     * @param phase the new phase
     */
    public void setPhase(ProcessState phase) {
        this.phase = phase;
    }

    /**
     * Gets the phase.
     *
     * @return the phase
     */
    public ProcessState getPhase() {
        return phase;
    }

    /**
     * Sets the status.
     *
     * @param status the new status
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Gets the status.
     *
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the progress.
     *
     * @param progress the new progress
     */
    public void setProgress(float progress) {
        this.progress = progress;
    }

    /**
     * Gets the progress.
     *
     * @return the progress
     */
    public float getProgress() {
        return progress;
    }

    /**
     * Sets the result.
     *
     * @param result the new result
     */
    public void setResult(String result) {
        this.result = result;
    }

    /**
     * Gets the result.
     *
     * @return the result
     */
    public String getResult() {
        return result;
    }

    /**
     * Hash code.
     *
     * @return the int
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterId == null) ? 0 : clusterId.hashCode());
        result = prime * result + ((executionId == null) ? 0 : executionId.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((phase == null) ? 0 : phase.hashCode());
        result = prime * result + Float.floatToIntBits(progress);
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        return result;
    }

    /**
     * Equals.
     *
     * @param obj the obj
     * @return true, if successful
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ProcessDescriptor)) {
            return false;
        }
        ProcessDescriptor other = (ProcessDescriptor) obj;
        if (clusterId == null) {
            if (other.clusterId != null) {
                return false;
            }
        } else if (!clusterId.equals(other.clusterId)) {
            return false;
        }
        if (executionId == null) {
            if (other.executionId != null) {
                return false;
            }
        } else if (!executionId.equals(other.executionId)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (phase != other.phase) {
            return false;
        }
        if (Float.floatToIntBits(progress) != Float.floatToIntBits(other.progress)) {
            return false;
        }
        if (status == null) {
            if (other.status != null) {
                return false;
            }
        } else if (!status.equals(other.status)) {
            return false;
        }
        return true;
    }

    /**
     * To string.
     *
     * @return the string
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProcessDescriptor [");
        if (id != null)
            builder.append("id=").append(id).append(", ");
        if (clusterId != null)
            builder.append("clusterId=").append(clusterId).append(", ");
        if (executionId != null)
            builder.append("executionId=").append(executionId).append(", ");
        if (phase != null)
            builder.append("phase=").append(phase).append(", ");
        if (status != null)
            builder.append("status=").append(status).append(", ");
        builder.append("progress=").append(progress);
        builder.append("]");
        return builder.toString();
    }

}
