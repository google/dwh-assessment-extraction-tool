package com.google.integration;

import org.junit.Test;

public class IntegrationTests {

    @Test
    public void integrationTest() {
        System.out.println(new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName());
    }
}
