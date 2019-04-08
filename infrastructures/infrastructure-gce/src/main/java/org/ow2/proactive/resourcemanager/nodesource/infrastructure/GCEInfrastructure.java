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
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.common.Configurable;
import org.ow2.proactive.resourcemanager.nodesource.infrastructure.util.LinuxInitScriptGenerator;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;


public class GCEInfrastructure extends AbstractAddonInfrastructure {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(GCEInfrastructure.class);

    public static final String INSTANCE_TAG_NODE_PROPERTY = "instanceTag";

    public static final String INFRASTRUCTURE_TYPE = "google-compute-engine";

    private transient LinuxInitScriptGenerator linuxInitScriptGenerator = new LinuxInitScriptGenerator();

    @Configurable(fileBrowser = true, description = "The JSON key file path of your Google Cloud Platform service account")
    protected GCECredential gceCredential = null;

    @Configurable(description = "Total instance to create")
    protected int numberOfInstances = 1;

    @Configurable(description = "Total nodes to create per instance")
    protected int numberOfNodesPerInstance = 1;

    @Configurable(description = "The virtual machine username")
    protected String vmUsername = null;

    @Configurable(fileBrowser = true, description = "The public key for accessing the virtual machine")
    protected String vmPublicKey = null;

    @Configurable(fileBrowser = true, description = "The private key for accessing the virtual machine")
    protected String vmPrivateKey = null;

    @Configurable(description = "Resource manager hostname or ip address (must be accessible from nodes)")
    protected String rmHostname = generateDefaultRMHostname();

    @Configurable(description = "Connector-iaas URL")
    protected String connectorIaasURL = "http://" + generateDefaultRMHostname() + ":8080/connector-iaas";

    @Configurable(description = "Additional Java command properties (e.g. \"-Dpropertyname=propertyvalue\")")
    protected String additionalProperties = "-Dproactive.useIPaddress=true";

    @Configurable(description = "The image of the virtual machine")
    protected String image = "debian-9-stretch-v20190326";

    @Configurable(description = "The region of the virtual machine")
    protected String region = "us-central1-a";

    @Configurable(description = "The minumum RAM required (in Mega Bytes) for each virtual machine")
    protected int ram = 1740;

    @Configurable(description = "The minimum number of CPU cores required for each virtual machine")
    protected int cores = 1;

    @Override
    public void configure(Object... parameters) {
        logger.info("updated");
        logger.info("Validating parameters : " + parameters);
        validate(parameters);

        int parameterIndex = 0;

        this.gceCredential = getCredentialFromJsonKeyFile((byte[]) parameters[parameterIndex++]);
        this.numberOfInstances = Integer.parseInt(parameters[parameterIndex++].toString().trim());
        this.numberOfNodesPerInstance = Integer.parseInt(parameters[parameterIndex++].toString().trim());
        this.vmUsername = parameters[parameterIndex++].toString().trim();
        this.vmPublicKey = new String((byte[]) parameters[parameterIndex++]);
        this.vmPrivateKey = new String((byte[]) parameters[parameterIndex++]);
        this.rmHostname = parameters[parameterIndex++].toString().trim();
        this.connectorIaasURL = parameters[parameterIndex++].toString().trim();
        this.additionalProperties = parameters[parameterIndex++].toString().trim();
        this.image = parameters[parameterIndex++].toString().trim();
        this.region = parameters[parameterIndex++].toString().trim();
        this.ram = Integer.parseInt(parameters[parameterIndex++].toString().trim());
        this.cores = Integer.parseInt(parameters[parameterIndex++].toString().trim());

        connectorIaasController = new ConnectorIaasController(connectorIaasURL, INFRASTRUCTURE_TYPE);
    }

    private void validate(Object[] parameters) {
        int parameterIndex = 0;
        if (parameters == null || parameters.length < 13) {
            throw new IllegalArgumentException("Invalid parameters for GCEInfrastructure creation");
        }
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "Google Cloud Platform service account must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The number of instances to create must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The number of nodes per instance to deploy must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The virtual machine username must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The public key for accessing the virtual machine must be specified");
        throwIllegalArgumentExceptionIfNull(parameters[parameterIndex++],
                                            "The private key for accessing the virtual machine must be specified");
        // rmHostname
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // connectorIaasURL
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // additionalProperties
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // image
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "debian-9-stretch-v20190326";
        }
        // region
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // ram
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "1740";
        }
        // cores
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "1";
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

        for (int i = 1; i <= numberOfInstances; i++) {

            String instanceTag = getInfrastructureId() + "-" + i;

            List<String> scripts = linuxInitScriptGenerator.buildScript(instanceTag,
                                                                        getRmUrl(),
                                                                        rmHostname,
                                                                        INSTANCE_TAG_NODE_PROPERTY,
                                                                        additionalProperties,
                                                                        nodeSource.getName(),
                                                                        numberOfNodesPerInstance);

            Set<String> instancesIds = connectorIaasController.createGCEInstances(getInfrastructureId(),
                                                                                  instanceTag,
                                                                                  1,
                                                                                  vmUsername,
                                                                                  vmPublicKey,
                                                                                  vmPrivateKey,
                                                                                  scripts,
                                                                                  image,
                                                                                  region,
                                                                                  ram,
                                                                                  cores);

            logger.info("Instances ids created: " + instancesIds);
        }

    }

    @Override
    public void acquireAllNodes() {
        acquireNode();
    }

    @Override
    public void notifyAcquiredNode(Node node) throws RMException {
        String instanceId = getInstanceIdProperty(node);

        addNewNodeForInstance(instanceId, node.getNodeInformation().getName());
    }

    @Override
    public void removeNode(Node node) throws RMException {
        String instanceId = getInstanceIdProperty(node);

        try {
            node.getProActiveRuntime().killNode(node.getNodeInformation().getName());
        } catch (Exception e) {
            logger.warn("Unable to remove the node '" + node.getNodeInformation().getName() + "' with error: " + e);
        }

        unregisterNodeAndRemoveInstanceIfNeeded(instanceId,
                                                node.getNodeInformation().getName(),
                                                getInfrastructureId(),
                                                true);
    }

    @Override
    protected String getInstanceIdProperty(Node node) throws RMException {
        try {
            return node.getProperty(INSTANCE_TAG_NODE_PROPERTY);
        } catch (ProActiveException e) {
            throw new RMException(e);
        }
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

    private String generateDefaultRMHostname() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            logger.warn(e);
            return "localhost";
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
