package com.qiwi.thrift.consul;

import com.ecwid.consul.v1.ConsulClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


@SuppressWarnings("MagicNumber")
public class ConsulParameterSourceTest {

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        ThriftConsulConfig config = new ThriftConsulConfig.Builder().setApplicationName("test").build();
        ConsulClient consulClient = mock(ConsulClient.class);
        new ConsulParameterSource(
                consulClient,
                config,
                "test/path/"
        );
    }

    @Test(groups = "unit")
    public void getPath() throws Exception {

    }

    @Test(groups = "unit")
    public void getString() throws Exception {

    }

}
