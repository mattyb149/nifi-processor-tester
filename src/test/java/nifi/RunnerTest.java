/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nifi;

import org.junit.Test;

import java.io.File;

/**
 * Unit tests for the Runner class
 */
public class RunnerTest {

    @Test
    public void testProc() {
        System.out.println(new File(".").getAbsolutePath());
        Runner.main(new String[]{"-nifi-path=src/test/resources/nifi_base", "-input=src/test/resources/input_files", "TestProcessor"});
    }

    @Test
    public void testProcWithAttrs() {
        System.out.println(new File(".").getAbsolutePath());
        Runner.main(new String[]{"-nifi-path=src/test/resources/nifi_base", "-input=src/test/resources/input_files",
                "-attrfile=src/test/resources/attrfiles/incoming_attributes.properties", "-attrs", "TestProcessor"});
    }

    @Test
    public void testProcWithAll() {
        System.out.println(new File(".").getAbsolutePath());
        Runner.main(new String[]{"-nifi-path=src/test/resources/nifi_base", "-input=src/test/resources/input_files", "-all", "TestProcessor"});
    }
}