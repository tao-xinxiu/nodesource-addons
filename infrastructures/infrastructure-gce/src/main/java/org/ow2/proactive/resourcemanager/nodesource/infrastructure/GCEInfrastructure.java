/*
 * ProActive Parallel Suite(TM):
 * The Open Source library for parallel and distributed
 * Workflows & Scheduling, Orchestration, Cloud Automation
 * and Big Data Analysis on Enterprise Grids & Clouds.
 *
 * Copyright (c) 2007 - 2017 ActiveEon
 * Contact: contact@activeeon.com
 *
 * This library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation: version 3 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 */
package org.ow2.proactive.resourcemanager.nodesource.infrastructure;

import static org.ow2.proactive.resourcemanager.core.properties.PAResourceManagerProperties.RM_CLOUD_INFRASTRUCTURES_DESTROY_INSTANCES_ON_SHUTDOWN;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.objectweb.proactive.core.node.Node;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.common.Configurable;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;


public class GCEInfrastructure extends AbstractAddonInfrastructure {
    private static final long serialVersionUID = 1L;

    public static final String INSTANCE_ID_NODE_PROPERTY = "instanceId";

    public static final String INFRASTRUCTURE_TYPE = "google-compute-engine";

    private static final String DEFAULT_HTTP_RM_URL = "http://localhost:8080";

    private static final Logger logger = Logger.getLogger(GCEInfrastructure.class);

    @Configurable(fileBrowser = true, description = "The JSON key file path of your Google Cloud Platform service account")
    protected GCECredential gceCredential = null;

    @Configurable(description = "Total instance to create")
    protected int numberOfInstances = 1;

    @Configurable(description = "Total nodes to create per instance")
    protected int numberOfNodesPerInstance = 1;

    @Configurable(description = "Resource manager HTTP URL (must be accessible from nodes)")
    protected String rmHttpUrl = generateDefaultHttpRMUrl();

    @Configurable(description = "Connector-iaas URL")
    protected String connectorIaasURL = generateDefaultHttpRMUrl() + "/connector-iaas";

    @Override
    public void configure(Object... parameters) {
        logger.info("Validating parameters : " + parameters);
        validate(parameters);

        int parameterIndex = 0;

        this.gceCredential = getCredentialFromJsonKeyFile((byte[]) parameters[parameterIndex++]);
        this.numberOfInstances = Integer.parseInt(parameters[parameterIndex++].toString().trim());
        this.numberOfNodesPerInstance = Integer.parseInt(parameters[parameterIndex++].toString().trim());
        this.rmHttpUrl = parameters[parameterIndex++].toString().trim();
        this.connectorIaasURL = parameters[parameterIndex++].toString().trim();

        connectorIaasController = new ConnectorIaasController(connectorIaasURL, INFRASTRUCTURE_TYPE);
    }

    private void validate(Object[] parameters) {
        int parameterIndex = 0;
        if (parameters == null || parameters.length < 3) {
            throw new IllegalArgumentException("Invalid parameters for GCEInfrastructure creation");
        }
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "GCE service account email must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++], "GCE service account key must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The number of instances to create must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The number of nodes per instance to deploy must be specified");
        // rmHttpUrl
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // connectorIaasURL
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
    }

    private void throwIllegalArgumentExceptionIfNull(Object parameter, String error) {
        if (parameter == null) {
            throw new IllegalArgumentException(error);
        }
    }

    private GCECredential getCredentialFromJsonKeyFile(byte[] credsFile) {
        try {
            final JsonObject json = new JsonParser().parse(new String(credsFile)).getAsJsonObject();
            String clientEmail = json.get("client_email").toString().trim().replace("\"", "");
            String privateKey = json.get("private_key").toString().replace("\"", "").replace("\\n", "\n");
            return new GCECredential(clientEmail, privateKey);
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't reading the JSON key file.");
        }

    }

    @Override
    public void acquireNode() {
        connectorIaasController.waitForConnectorIaasToBeUP();
        connectorIaasController.createInfrastructure(getInfrastructureId(),
                                                     gceCredential.clientEmail,
                                                     gceCredential.privateKey,
                                                     null,
                                                     RM_CLOUD_INFRASTRUCTURES_DESTROY_INSTANCES_ON_SHUTDOWN.getValueAsBoolean());
        connectorIaasController.createGCEInstances(getInfrastructureId(), getInfrastructureId(), numberOfInstances);
    }

    @Override
    public void acquireAllNodes() {
        acquireNode();
    }

    @Override
    protected void notifyAcquiredNode(Node arg0) throws RMException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void removeNode(Node arg0) throws RMException {
        throw new UnsupportedOperationException("");
    }

    @Override
    protected String getInstanceIdProperty(Node node) throws RMException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public String getDescription() {
        return "Handles nodes from the Google Compute Engine.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getDescription();
    }

    private String generateDefaultHttpRMUrl() {
        try {
            // best effort, may not work for all machines
            return "http://" + InetAddress.getLocalHost().getCanonicalHostName() + ":8080";
        } catch (UnknownHostException e) {
            logger.warn("Unable to retrieve local canonical hostname with error: " + e);
            return DEFAULT_HTTP_RM_URL;
        }
    }

    @Getter
    @AllArgsConstructor
    @ToString
    class GCECredential {
        String clientEmail;

        String privateKey;
    }
}
