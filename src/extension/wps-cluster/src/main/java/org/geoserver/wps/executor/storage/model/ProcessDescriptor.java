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
 *
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

    @Column(nullable = false, updatable = true)
    private String clusterId;

    @Column(nullable = false, updatable = true)
    private String executionId;

    @Enumerated(EnumType.ORDINAL)
    @Column(nullable = false, updatable = true)
    private ProcessState phase;

    @Lob
    @Column(nullable = false, updatable = true)
    private String status;

    @Column(nullable = false, updatable = true)
    private float progress;

    @Column(nullable = true, updatable = true)
    private String result;

    /**
     * Instantiates a new instance.
     */
    public ProcessDescriptor() {

    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setPhase(ProcessState phase) {
        this.phase = phase;
    }

    public ProcessState getPhase() {
        return phase;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public void setProgress(float progress) {
        this.progress = progress;
    }

    public float getProgress() {
        return progress;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }

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
