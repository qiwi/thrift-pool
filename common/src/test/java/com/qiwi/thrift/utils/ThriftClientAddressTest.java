package com.qiwi.thrift.utils;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;

public class ThriftClientAddressTest {
    @Test(groups = "unit", dataProvider = "parseDataProvider")
    public void parse(String address, String result) throws Exception {
        ThriftClientAddress addressParsed = ThriftClientAddress.parse(address);
        assertEquals(addressParsed.toString(), result);
    }

    @DataProvider
    public static Object[][] parseDataProvider() {
        return new Object[][] {
                {"test.osmp.ru,dc=s1,version=2.0", "test.osmp.ru:9090,dc=s1,version=2.0"},
                {"test.osmp.ru:3131,dc=s1,version=2.0", "test.osmp.ru:3131,dc=s1,version=2.0"},
                {"test.osmp.ru:3131,dc=s1,tag,version=2.0", "test.osmp.ru:3131,dc=s1,tag,version=2.0"},
                {"test.osmp.ru:3131,tag", "test.osmp.ru:3131,tag"},
                {"test.osmp.ru:3131", "test.osmp.ru:3131"},
                {"test.osmp.ru", "test.osmp.ru:9090"},
        };
    }

    @Test(groups = "unit", dataProvider = "parseErrorDataProvider", expectedExceptions = ThriftConnectionException.class)
    public void parseError(String address) throws Exception {
        ThriftClientAddress.parse(address);
    }

    @DataProvider
    public static Object[][] parseErrorDataProvider() {
        return new Object[][] {
                {",dc=s1,version=2.0"},
                {"dc=s1,version=2.0"},
                {"test.osmp.ru:31d31,dc=s1,version=2.0"},
         };
    }

    @Test(groups = "unit", expectedExceptions = NullPointerException.class)
    public void parseNPE() throws Exception {
        ThriftClientAddress.parse(null);
    }

    @Test(groups = "unit", expectedExceptions = ThriftConnectionException.class)
    public void parseError2() throws Exception {
        ThriftClientAddress.parse("test.osmp.ru:31d31");
    }

    @Test(groups = "unit")
    public void of() throws Exception {
        Supplier<ThriftClientAddress> supplier =
                ThriftClientAddress.supplier((name, defVal) -> "address".equals(name) ? "test.osmp.ru:3131" : defVal);
        assertEquals(supplier.get().toString(), "test.osmp.ru:3131");
    }

}
