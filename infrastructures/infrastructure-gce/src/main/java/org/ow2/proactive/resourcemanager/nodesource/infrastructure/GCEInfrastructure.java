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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.common.Configurable;
import org.ow2.proactive.resourcemanager.nodesource.infrastructure.util.LinuxInitScriptGenerator;

import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;


public class GCEInfrastructure extends AbstractAddonInfrastructure {

    public static final String INFRASTRUCTURE_TYPE = "google-compute-engine";

    public static final String INSTANCE_TAG_NODE_PROPERTY = "instanceTag";

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger(GCEInfrastructure.class);

    private static final String DEFAULT_IMAGE = "debian-9-stretch-v20190326";

    private static final String DEFAULT_REGION = "us-central1-a";

    private static final int DEFAULT_RAM = 1740;

    private static final int DEFAULT_CORES = 1;

    private static final boolean DESTROY_INSTANCES_ON_SHUTDOWN = true;

    private static final int JOB_STATE_REFRESH_RATE = 1000;

    private transient LinuxInitScriptGenerator linuxInitScriptGenerator = new LinuxInitScriptGenerator();

    @Configurable(fileBrowser = true, description = "The JSON key file path of your Google Cloud Platform service account")
    protected GCECredential gceCredential = null;

    @Configurable(description = "Total instance to create")
    protected int numberOfInstances = 1;

    @Configurable(description = "Total nodes to create per instance")
    protected int numberOfNodesPerInstance = 1;

    @Configurable(description = "(optional) The virtual machine username")
    protected String vmUsername = null;

    @Configurable(fileBrowser = true, description = "(optional) The public key for accessing the virtual machine")
    protected String vmPublicKey = null;

    @Configurable(fileBrowser = true, description = "(optional) The private key for accessing the virtual machine")
    protected String vmPrivateKey = null;

    @Configurable(description = "Resource manager hostname or ip address (must be accessible from nodes)")
    protected String rmHostname = generateDefaultRMHostname();

    @Configurable(description = "Connector-iaas URL")
    protected String connectorIaasURL = "http://" + generateDefaultRMHostname() + ":8080/connector-iaas";

    @Configurable(description = "Command used to download the node jar")
    protected String downloadCommand = linuxInitScriptGenerator.generateNodeDownloadCommand(rmHostname);

    @Configurable(description = "(optional) Additional Java command properties (e.g. \"-Dpropertyname=propertyvalue\")")
    protected String additionalProperties = "-Dproactive.useIPaddress=true";

    @Configurable(description = "(optional) The image of the virtual machine")
    protected String image = DEFAULT_IMAGE;

    @Configurable(description = "(optional) The region of the virtual machine")
    protected String region = DEFAULT_REGION;

    @Configurable(description = "(optional) The minimum RAM required (in Mega Bytes) for each virtual machine")
    protected int ram = DEFAULT_RAM;

    @Configurable(description = "(optional) The minimum number of CPU cores required for each virtual machine")
    protected int cores = DEFAULT_CORES;

    @Override
    public void configure(Object... parameters) {
        logger.info("Validating parameters : " + Arrays.toString(parameters));
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
        this.downloadCommand = parameters[parameterIndex++].toString().trim();
        this.additionalProperties = parameters[parameterIndex++].toString().trim();
        this.image = parameters[parameterIndex++].toString().trim();
        this.region = parameters[parameterIndex++].toString().trim();
        this.ram = Integer.parseInt(parameters[parameterIndex++].toString().trim());
        this.cores = Integer.parseInt(parameters[parameterIndex++].toString().trim());

        connectorIaasController = new ConnectorIaasController(connectorIaasURL, INFRASTRUCTURE_TYPE);
    }

    private void validate(Object[] parameters) {
        if (parameters == null || parameters.length < 14) {
            throw new IllegalArgumentException("Invalid parameters for GCEInfrastructure creation");
        }
        int parameterIndex = 0;
        // gceCredential
        if (parameters[parameterIndex++] == null) {
            throw new IllegalArgumentException("The Google Cloud Platform service account must be specified");
        }
        // numberOfInstances
        if (parameters[parameterIndex++] == null) {
            throw new IllegalArgumentException("The number of instances to create must be specified");
        }
        // numberOfNodesPerInstance
        if (parameters[parameterIndex++] == null) {
            throw new IllegalArgumentException("The number of nodes per instance to deploy must be specified");
        }
        // vmUsername
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // vmPublicKey
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // vmPrivateKey
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // rmHostname
        if (parameters[parameterIndex++] == null) {
            throw new IllegalArgumentException("The resource manager hostname must be specified");
        }
        // connectorIaasURL
        if (parameters[parameterIndex++] == null) {
            throw new IllegalArgumentException("The connector-iaas URL must be specified");
        }
        // downloadCommand
        if (parameters[parameterIndex++] == null) {
            throw new IllegalArgumentException("The command for downloading the node jar must be specified");
        }
        // additionalProperties
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = "";
        }
        // image
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = DEFAULT_IMAGE;
        }
        // region
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = DEFAULT_REGION;
        }
        // ram
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = String.valueOf(DEFAULT_RAM);
        }
        // cores
        if (parameters[parameterIndex] == null) {
            parameters[parameterIndex++] = String.valueOf(DEFAULT_CORES);
        }
    }

    private GCECredential getCredentialFromJsonKeyFile(byte[] credsFile) {
        try {
            final JsonObject json = new JsonParser().parse(new String(credsFile)).getAsJsonObject();
            String clientEmail = json.get("client_email").toString().trim().replace("\"", "");
            String privateKey = json.get("private_key").toString().replace("\"", "").replace("\\n", "\n");
            return new GCECredential(clientEmail, privateKey);
        } catch (Exception e) {
            logger.error(e);
            throw new IllegalArgumentException("Can't reading the GCE service account JSON key file: " +
                                               new String(credsFile));
        }

    }

    @Override
    public void acquireNode() {
        connectorIaasController.waitForConnectorIaasToBeUP();

        connectorIaasController.createInfrastructure(getInfrastructureId(),
                                                     gceCredential.clientEmail,
                                                     gceCredential.privateKey,
                                                     null,
                                                     DESTROY_INSTANCES_ON_SHUTDOWN);

        // the initial scripts to be executed on each node requires the identification of the instance (i.e., instanceTag), which can be retrieved through its hostname on each instance.
        final String initCmdInstanceTag = "$HOSTNAME";
        List<String> scripts = linuxInitScriptGenerator.buildScript(initCmdInstanceTag,
                                                                    getRmUrl(),
                                                                    rmHostname,
                                                                    INSTANCE_TAG_NODE_PROPERTY,
                                                                    additionalProperties,
                                                                    nodeSource.getName(),
                                                                    numberOfNodesPerInstance);

        Set<String> instancesIds = connectorIaasController.createGCEInstances(getInfrastructureId(),
                                                                              getInfrastructureId(),
                                                                              numberOfInstances,
                                                                              vmUsername,
                                                                              vmPublicKey,
                                                                              vmPrivateKey,
                                                                              scripts,
                                                                              image,
                                                                              region,
                                                                              ram,
                                                                              cores);

        logger.info("Instances created: " + instancesIds);
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

    @SuppressWarnings("unchecked")
    @Override
    protected void unregisterNodeAndRemoveInstanceIfNeeded(final String instanceTag, final String nodeName,
            final String infrastructureId, final boolean terminateInstanceIfEmpty) {
        setPersistedInfraVariable(() -> {
            // first read from the runtime variables map
            nodesPerInstance = (Map<String, Set<String>>) persistedInfraVariables.get(NODES_PER_INSTANCES_KEY);
            // make modifications to the nodesPerInstance map
            if (nodesPerInstance.get(instanceTag) != null) {
                nodesPerInstance.get(instanceTag).remove(nodeName);
                logger.info("Removed node : " + nodeName);
                if (nodesPerInstance.get(instanceTag).isEmpty()) {
                    if (terminateInstanceIfEmpty) {
                        connectorIaasController.terminateInstanceByTag(infrastructureId, instanceTag);
                        logger.info("Instance terminated: " + instanceTag);
                    }
                    nodesPerInstance.remove(instanceTag);
                    logger.info("Removed instance : " + instanceTag);
                }
                // finally write to the runtime variable map
                persistedInfraVariables.put(NODES_PER_INSTANCES_KEY, Maps.newHashMap(nodesPerInstance));
            } else {
                logger.error("Cannot remove node " + nodeName + " because instance " + instanceTag +
                             " is not registered");
            }
            return null;
        });
    }

    /**
     * parse the google-compute-engine instance tag from instance id
     *
     * @param instanceId e.g., https://www.googleapis.com/compute/v1/projects/fifth-totality-235316/zones/us-central1-a/instances/gce-afa
     * @return instanceTag e.g., gce-afa
     */
    private static String parseGCEInstanceTagFromId(String instanceId) {
        String[] instanceIdSplit = instanceId.split("/");
        return instanceIdSplit[instanceIdSplit.length - 1];
    }

    @Getter
    @AllArgsConstructor
    @ToString
    class GCECredential {
        String clientEmail;

        String privateKey;
    }
}
