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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.objectweb.proactive.core.node.NodeInformation;
import org.objectweb.proactive.core.runtime.ProActiveRuntime;
import org.ow2.proactive.resourcemanager.authentication.Client;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.NodeSource;
import org.ow2.proactive.resourcemanager.nodesource.infrastructure.util.LinuxInitScriptGenerator;
import org.ow2.proactive.resourcemanager.rmnode.RMDeployingNode;
import org.python.google.common.collect.Sets;


/**
 * @author ActiveEon Team
 * @since 16/04/19
 */
public class GCEInfrastructureTest {
    private static final String CLIENT_EMAIL = "user@proj.iam.gserviceaccount.com";

    private static final String PRIVATE_KEY_RAW = "--BEGIN PRIVATE KEY--\\nPrivateKey\\n--END PRIVATE KEY--\\n";

    private static final String PRIVATE_KEY = PRIVATE_KEY_RAW.replace("\\n", "\n");

    private static final byte[] CREDENTIAL_FILE = ("{\"private_key\": \"" + PRIVATE_KEY_RAW +
                                                   "\", \"client_email\": \"" + CLIENT_EMAIL + "\"}").getBytes();

    private static final int NUMBER_INSTANCES = 2;

    private static final int NUMBER_NODES_PER_INSTANCE = 3;

    private static final String VM_USERNAME = "username";

    private static final String VM_PUBLIC_KEY = "ssh-rsa PUBLICKEY user@pc";

    private static final byte[] VM_PUBLIC_KEY_BYTES = VM_PUBLIC_KEY.getBytes();

    private static final String VM_PRIVATE_KEY = "--BEGIN PRIVATE KEY--\\nVM\\nPrivate\\nKey\\n--END PRIVATE KEY--\\n";

    private static final byte[] VM_PRIVATE_KEY_BYTES = VM_PRIVATE_KEY.getBytes();

    private static final String RM_HOSTNAME = "test.activeeon.com";

    private static final String CONNECTOR_IAAS_URL = "http://localhost:8088/connector-iaas";

    private static final String DOWNLOAD_COMMAND = "wget -nv test.activeeon.com/rest/node.jar";

    private static final String ADDITIONAL_PROPERTIES = "-Dproactive.useIPaddress=true";

    private static final String IMAGE = "gce-debian-9";

    private static final String REGION = "us-central1-a";

    private static final int RAM = 1740;

    private static final int CORES = 1;

    private static final int NODE_TIMEOUT = 120000;

    private static final String INFRASTRUCTURE_ID = "infrastructure_id";

    private static final boolean DESTROY_INSTANCES_ON_SHUTDOWN = true;

    private static final List<String> initScripts = Arrays.asList(DOWNLOAD_COMMAND, "node start cmd");

    @InjectMocks
    @Spy
    private GCEInfrastructure gceInfrastructure;

    @Mock
    private LinuxInitScriptGenerator linuxInitScriptGenerator;

    @Mock
    private ConnectorIaasController connectorIaasController;

    @Mock
    private NodeSource nodeSource;

    @Mock
    private Node node;

    @Mock
    private NodeInformation nodeInformation;

    @Mock
    private ProActiveRuntime proActiveRuntime;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        gceInfrastructure.initializePersistedInfraVariables();
    }

    @Test
    public void testInitialParameters() {
        assertThat(gceInfrastructure.gceCredential, is(nullValue()));
        assertThat(gceInfrastructure.numberOfInstances, is(1));
        assertThat(gceInfrastructure.numberOfNodesPerInstance, is(1));
        assertThat(gceInfrastructure.vmUsername, is(nullValue()));
        assertThat(gceInfrastructure.vmPublicKey, is(nullValue()));
        assertThat(gceInfrastructure.vmPrivateKey, is(nullValue()));
        assertThat(gceInfrastructure.rmHostname, is(not(nullValue())));
        assertThat(gceInfrastructure.connectorIaasURL,
                   is("http://" + gceInfrastructure.rmHostname + ":8080/connector-iaas"));
        assertThat(gceInfrastructure.downloadCommand,
                   is("wget -nv " + gceInfrastructure.rmHostname + ":8080/rest/node.jar"));
        assertThat(gceInfrastructure.additionalProperties, is(not(nullValue())));
        assertThat(gceInfrastructure.image, is(not(nullValue())));
        assertThat(gceInfrastructure.region, is(not(nullValue())));
        assertThat(gceInfrastructure.ram, is(greaterThanOrEqualTo(RAM)));
        assertThat(gceInfrastructure.cores, is(CORES));
        assertThat(gceInfrastructure.nodeTimeout, is(not(nullValue())));
    }

    @Test
    public void testConfigure() {
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);

        assertThat(gceInfrastructure.gceCredential.clientEmail, is(CLIENT_EMAIL));
        assertThat(gceInfrastructure.gceCredential.privateKey, is(PRIVATE_KEY));
        assertThat(gceInfrastructure.numberOfInstances, is(NUMBER_INSTANCES));
        assertThat(gceInfrastructure.numberOfNodesPerInstance, is(NUMBER_NODES_PER_INSTANCE));
        assertThat(gceInfrastructure.vmUsername, is(VM_USERNAME));
        assertThat(gceInfrastructure.vmPublicKey, is(VM_PUBLIC_KEY));
        assertThat(gceInfrastructure.vmPrivateKey, is(VM_PRIVATE_KEY));
        assertThat(gceInfrastructure.rmHostname, is(RM_HOSTNAME));
        assertThat(gceInfrastructure.connectorIaasURL, is(CONNECTOR_IAAS_URL));
        assertThat(gceInfrastructure.downloadCommand, is(DOWNLOAD_COMMAND));
        assertThat(gceInfrastructure.additionalProperties, is(ADDITIONAL_PROPERTIES));
        assertThat(gceInfrastructure.image, is(IMAGE));
        assertThat(gceInfrastructure.region, is(REGION));
        assertThat(gceInfrastructure.ram, is(RAM));
        assertThat(gceInfrastructure.cores, is(CORES));
        assertThat(gceInfrastructure.nodeTimeout, is(NODE_TIMEOUT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void tesConfigureNotEnoughParameters() {
        gceInfrastructure.configure(NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tesConfigureNullMandatoryParameters() {
        gceInfrastructure.configure(null,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
    }

    @Test
    public void testAcquireNode() {
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        when(nodeSource.getName()).thenReturn(INFRASTRUCTURE_ID);
        when(linuxInitScriptGenerator.buildScript(anyString(),
                                                  anyString(),
                                                  anyString(),
                                                  anyString(),
                                                  anyString(),
                                                  anyString(),
                                                  anyString(),
                                                  anyInt())).thenReturn(initScripts);

        gceInfrastructure.acquireNode();

        verify(connectorIaasController, times(1)).waitForConnectorIaasToBeUP();
        verify(connectorIaasController, times(1)).createInfrastructure(INFRASTRUCTURE_ID,
                                                                       CLIENT_EMAIL,
                                                                       PRIVATE_KEY,
                                                                       null,
                                                                       DESTROY_INSTANCES_ON_SHUTDOWN);
        verify(connectorIaasController, times(1)).createGCEInstances(INFRASTRUCTURE_ID,
                                                                     INFRASTRUCTURE_ID,
                                                                     NUMBER_INSTANCES,
                                                                     VM_USERNAME,
                                                                     VM_PUBLIC_KEY,
                                                                     VM_PRIVATE_KEY,
                                                                     initScripts,
                                                                     IMAGE,
                                                                     REGION,
                                                                     RAM,
                                                                     CORES);
    }

    @Test
    public void testNotifyAcquiredNode() throws ProActiveException, RMException {
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        final String instanceTag = "instance-tag";
        final String nodeName = "node-name";
        when(node.getProperty(GCEInfrastructure.INSTANCE_TAG_NODE_PROPERTY)).thenReturn(instanceTag);
        when(node.getNodeInformation()).thenReturn(nodeInformation);
        when(nodeInformation.getName()).thenReturn(nodeName);

        gceInfrastructure.notifyAcquiredNode(node);

        assertThat(gceInfrastructure.getNodesPerInstancesMapCopy().get(instanceTag).isEmpty(), is(false));
        assertThat(gceInfrastructure.getNodesPerInstancesMapCopy().get(instanceTag).size(), is(1));
        assertThat(gceInfrastructure.getNodesPerInstancesMapCopy().get(instanceTag).contains(nodeName), is(true));
    }

    @Test
    public void testRemoveNode() throws RMException, ProActiveException {
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        final String instanceTag = "instance-tag";
        final String nodeName = "node-name";
        when(node.getProperty(GCEInfrastructure.INSTANCE_TAG_NODE_PROPERTY)).thenReturn(instanceTag);
        when(node.getProActiveRuntime()).thenReturn(proActiveRuntime);
        when(node.getNodeInformation()).thenReturn(nodeInformation);
        when(nodeInformation.getName()).thenReturn(nodeName);
        when(nodeSource.getName()).thenReturn(INFRASTRUCTURE_ID);
        gceInfrastructure.getNodesPerInstancesMap().put(instanceTag, Sets.newHashSet());

        gceInfrastructure.removeNode(node);

        verify(proActiveRuntime, times(1)).killNode(nodeName);
        verify(connectorIaasController).terminateInstanceByTag(INFRASTRUCTURE_ID, instanceTag);
        assertThat(gceInfrastructure.getNodesPerInstancesMap().isEmpty(), is(true));
    }

    @Test
    public void testNotifyDeployingNodeLostShouldDeleteInstanceGivenNoOtherNodes() {
        final String instanceTag = "instance-tag";
        final String nodeName = instanceTag;
        final String nodeUrl = String.format("deploying://%s/%s", INFRASTRUCTURE_ID, nodeName);
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        RMDeployingNode node = new RMDeployingNode(nodeName, new NodeSource(), "", new Client());
        doReturn(node).when(gceInfrastructure).getDeployingOrLostNode(anyString());
        gceInfrastructure.getNodesPerInstancesMap().put(instanceTag, Sets.newHashSet());
        doReturn(Arrays.asList(node)).when(gceInfrastructure).getDeployingAndLostNodes();
        when(nodeSource.getName()).thenReturn(INFRASTRUCTURE_ID);

        gceInfrastructure.notifyDeployingNodeLost(nodeUrl);

        verify(connectorIaasController, times(1)).terminateInstanceByTag(INFRASTRUCTURE_ID, instanceTag);
    }

    @Test
    public void testNotifyDeployingNodeLostShouldNotDeleteInstanceGivenOtherPersistedNode() {
        final String instanceTag = "instance-tag";
        final String nodeName = instanceTag;
        final String nodeUrl = String.format("deploying://%s/%s", INFRASTRUCTURE_ID, nodeName);
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        RMDeployingNode node = new RMDeployingNode(nodeName, new NodeSource(), "", new Client());
        doReturn(node).when(gceInfrastructure).getDeployingOrLostNode(anyString());
        gceInfrastructure.getNodesPerInstancesMap()
                         .put(instanceTag, new HashSet<>(Arrays.asList("pamr://4097/node_0", "pamr://4097/node_1")));

        gceInfrastructure.notifyDeployingNodeLost(nodeUrl);

        verify(connectorIaasController, never()).terminateInstanceByTag(INFRASTRUCTURE_ID, instanceTag);
    }

    @Test
    public void testNotifyDeployingNodeLostShouldNotDeleteInstanceGivenOtherDeployingNode() {
        final String instanceTag = "instance-tag";
        final String nodeName1 = instanceTag + "_0";
        final String nodeName2 = instanceTag + "_1";
        final String nodeUrl1 = String.format("deploying://%s/%s", INFRASTRUCTURE_ID, nodeName1);
        final String nodeUrl2 = String.format("deploying://%s/%s", INFRASTRUCTURE_ID, nodeName2);
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        RMDeployingNode node1 = new RMDeployingNode(nodeName1, new NodeSource(), "", new Client());
        doReturn(node1).when(gceInfrastructure).getDeployingOrLostNode(anyString());
        gceInfrastructure.getNodesPerInstancesMap().put(instanceTag, Sets.newHashSet());
        RMDeployingNode node2 = new RMDeployingNode(nodeName2, new NodeSource(), "", new Client());
        doReturn(Arrays.asList(node1, node2)).when(gceInfrastructure).getDeployingAndLostNodes();
        when(nodeSource.getName()).thenReturn(INFRASTRUCTURE_ID);

        gceInfrastructure.notifyDeployingNodeLost(nodeUrl1);

        verify(connectorIaasController, never()).terminateInstanceByTag(INFRASTRUCTURE_ID, instanceTag);
    }

    @Test
    public void testNotifyDeployingNodeLostShouldDeleteInstanceGivenOtherLostNode() {
        final String instanceTag = "instance-tag";
        final String nodeName1 = instanceTag + "_0";
        final String nodeName2 = instanceTag + "_1";
        final String nodeUrl1 = String.format("deploying://%s/%s", INFRASTRUCTURE_ID, nodeName1);
        final String nodeUrl2 = String.format("deploying://%s/%s", INFRASTRUCTURE_ID, nodeName2);
        gceInfrastructure.configure(CREDENTIAL_FILE,
                                    NUMBER_INSTANCES,
                                    NUMBER_NODES_PER_INSTANCE,
                                    VM_USERNAME,
                                    VM_PUBLIC_KEY_BYTES,
                                    VM_PRIVATE_KEY_BYTES,
                                    RM_HOSTNAME,
                                    CONNECTOR_IAAS_URL,
                                    DOWNLOAD_COMMAND,
                                    ADDITIONAL_PROPERTIES,
                                    IMAGE,
                                    REGION,
                                    RAM,
                                    CORES,
                                    NODE_TIMEOUT);
        // re-assign needed because gceInfrastructure.configure new the object gceInfrastructure.connectorIaasController
        gceInfrastructure.connectorIaasController = connectorIaasController;
        RMDeployingNode node1 = new RMDeployingNode(nodeName1, new NodeSource(), "", new Client());
        doReturn(node1).when(gceInfrastructure).getDeployingOrLostNode(anyString());
        gceInfrastructure.getNodesPerInstancesMap().put(instanceTag, Sets.newHashSet());
        RMDeployingNode node2 = new RMDeployingNode(nodeName2, new NodeSource(), "", new Client());
        node2.setLost();
        doReturn(Arrays.asList(node1, node2)).when(gceInfrastructure).getDeployingAndLostNodes();
        when(nodeSource.getName()).thenReturn(INFRASTRUCTURE_ID);

        gceInfrastructure.notifyDeployingNodeLost(nodeUrl1);

        verify(connectorIaasController, times(1)).terminateInstanceByTag(INFRASTRUCTURE_ID, instanceTag);
    }
}
