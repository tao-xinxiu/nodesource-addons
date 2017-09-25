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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.objectweb.proactive.core.ProActiveException;
import org.objectweb.proactive.core.node.Node;
import org.objectweb.proactive.core.node.NodeInformation;
import org.objectweb.proactive.core.runtime.ProActiveRuntime;
import org.ow2.proactive.resourcemanager.exception.RMException;
import org.ow2.proactive.resourcemanager.nodesource.NodeSource;
import org.python.google.common.collect.Sets;


public class AzureScaleSetInfrastructureTest {

    private AzureScaleSetInfrastructure AzureScaleSetInfrastructure;

    @Mock
    private ConnectorIaasController connectorIaasController;

    @Mock
    private NodeSource nodeSource;

    @Mock
    private Node node;

    @Mock
    private ProActiveRuntime proActiveRuntime;

    @Mock
    private NodeInformation nodeInformation;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        AzureScaleSetInfrastructure = new AzureScaleSetInfrastructure();

    }

    @Test
    public void testDefaultValuesOfAllParameters() {
        assertThat(AzureScaleSetInfrastructure.clientId, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.secret, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.domain, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.subscriptionId, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.authenticationEndpoint, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.managementEndpoint, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.resourceManagerEndpoint, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.graphEndpoint, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.rmHostname, is(not(nullValue())));
        assertThat(AzureScaleSetInfrastructure.connectorIaasURL,
                   is("http://" + AzureScaleSetInfrastructure.rmHostname + ":8080/connector-iaas"));
        assertThat(AzureScaleSetInfrastructure.image, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.vmSizeType, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.vmUsername, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.vmPassword, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.vmPublicKey, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.resourceGroup, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.region, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.numberOfInstances, is(1));
        assertThat(AzureScaleSetInfrastructure.numberOfNodesPerInstance, is(1));
        assertThat(AzureScaleSetInfrastructure.customScriptURL, is("http://8080/customScript.sh"));
        assertThat(AzureScaleSetInfrastructure.privateNetworkCIDR, is(nullValue()));
        assertThat(AzureScaleSetInfrastructure.staticPublicIP, is(true));
        assertThat(AzureScaleSetInfrastructure.additionalProperties, is("-Dproactive.useIPaddress=true"));
    }

    @Test
    public void testConfigureDoNotThrowIllegalArgumentExceptionWithValidParameters() {
        when(nodeSource.getName()).thenReturn("Node source Name");
        AzureScaleSetInfrastructure.nodeSource = nodeSource;

        try {
            AzureScaleSetInfrastructure.configure("clientId",
                                                  "secret",
                                                  "domain",
                                                  "subscriptionId",
                                                  "authenticationEndpoint",
                                                  "managementEndpoint",
                                                  "resourceManagerEndpoint",
                                                  "graphEndpoint",
                                                  "test.activeeon.com",
                                                  "http://localhost:8088/connector-iaas",
                                                  "image",
                                                  "Standard_D1_v2",
                                                  "vmUsername",
                                                  "vmPassword",
                                                  "vmPublicKey",
                                                  "resourceGroup",
                                                  "region",
                                                  "2",
                                                  "3",
                                                  "http://8080/customScript.sh",
                                                  "192.168.1.0/24",
                                                  true,
                                                  "-Dnew=value");
            Assert.assertTrue(Boolean.TRUE);
        } catch (IllegalArgumentException e) {
            fail("NPE not thrown");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void tesConfigureNotEnoughParameters() {

        when(nodeSource.getName()).thenReturn("Node source Name");
        AzureScaleSetInfrastructure.nodeSource = nodeSource;

        AzureScaleSetInfrastructure.configure("clientId", "secret");
    }

    @Test(expected = IllegalArgumentException.class)
    public void tesConfigureWithANullArgument() {

        when(nodeSource.getName()).thenReturn("Node source Name");
        AzureScaleSetInfrastructure.nodeSource = nodeSource;

        AzureScaleSetInfrastructure.configure("clientId",
                                              "secret",
                                              "domain",
                                              "subscriptionId",
                                              "authenticationEndpoint",
                                              "managementEndpoint",
                                              "resourceManagerEndpoint",
                                              "graphEndpoint",
                                              "test.activeeon.com",
                                              "http://localhost:8088/connector-iaas",
                                              null,
                                              "Standard_D1_v2",
                                              "vmUsername",
                                              "vmPassword",
                                              "vmPublicKey",
                                              "resourceGroup",
                                              "region",
                                              "2",
                                              "3",
                                              "http://8080/customScript.sh",
                                              "192.168.1.0/24",
                                              true,
                                              "-Dnew=value");
    }

    @Test
    public void testRemoveNode() throws ProActiveException, RMException {
        when(nodeSource.getName()).thenReturn("Node source Name");
        AzureScaleSetInfrastructure.nodeSource = nodeSource;

        AzureScaleSetInfrastructure.configure("clientId",
                                              "secret",
                                              "domain",
                                              "subscriptionId",
                                              "authenticationEndpoint",
                                              "managementEndpoint",
                                              "resourceManagerEndpoint",
                                              "graphEndpoint",
                                              "test.activeeon.com",
                                              "http://localhost:8088/connector-iaas",
                                              "image",
                                              "Standard_D1_v2",
                                              "vmUsername",
                                              "vmPassword",
                                              "vmPublicKey",
                                              "resourceGroup",
                                              "region",
                                              "2",
                                              "3",
                                              "http://8080/customScript.sh",
                                              "192.168.1.0/24",
                                              true,
                                              "-Dnew=value");

        AzureScaleSetInfrastructure.connectorIaasController = connectorIaasController;

        when(node.getProperty(AzureScaleSetInfrastructure.INSTANCE_ID_NODE_PROPERTY)).thenReturn("123");

        when(node.getNodeInformation()).thenReturn(nodeInformation);

        when(node.getProActiveRuntime()).thenReturn(proActiveRuntime);

        when(nodeInformation.getName()).thenReturn("nodename");

        AzureScaleSetInfrastructure.nodesPerInstances.put("123", Sets.newHashSet("nodename"));

        AzureScaleSetInfrastructure.removeNode(node);

        verify(proActiveRuntime).killNode("nodename");

        verify(connectorIaasController).terminateInstance("node_source_name", "123");

        assertThat(AzureScaleSetInfrastructure.nodesPerInstances.isEmpty(), is(true));

    }

    @Test
    public void testThatNotifyAcquiredNodeMethodFillsTheNodesMapCorrectly() throws ProActiveException, RMException {

        when(nodeSource.getName()).thenReturn("Node source Name");
        AzureScaleSetInfrastructure.nodeSource = nodeSource;
        AzureScaleSetInfrastructure.configure("clientId",
                                              "secret",
                                              "domain",
                                              "subscriptionId",
                                              "authenticationEndpoint",
                                              "managementEndpoint",
                                              "resourceManagerEndpoint",
                                              "graphEndpoint",
                                              "test.activeeon.com",
                                              "http://localhost:8088/connector-iaas",
                                              "image",
                                              "Standard_D1_v2",
                                              "vmUsername",
                                              "vmPassword",
                                              "vmPublicKey",
                                              "resourceGroup",
                                              "region",
                                              "2",
                                              "3",
                                              "http://8080/customScript.sh",
                                              "192.168.1.0/24",
                                              true,
                                              "-Dnew=value");

        AzureScaleSetInfrastructure.connectorIaasController = connectorIaasController;

        when(node.getProperty(AzureScaleSetInfrastructure.INSTANCE_ID_NODE_PROPERTY)).thenReturn("123");

        when(node.getNodeInformation()).thenReturn(nodeInformation);

        when(nodeInformation.getName()).thenReturn("nodename");

        AzureScaleSetInfrastructure.notifyAcquiredNode(node);

        assertThat(AzureScaleSetInfrastructure.nodesPerInstances.get("123").isEmpty(), is(false));
        assertThat(AzureScaleSetInfrastructure.nodesPerInstances.get("123").size(), is(1));
        assertThat(AzureScaleSetInfrastructure.nodesPerInstances.get("123").contains("nodename"), is(true));
    }
}
