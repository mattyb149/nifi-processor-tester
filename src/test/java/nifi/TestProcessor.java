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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestProcessor extends AbstractProcessor {

    /**
     * A relationship indicating flow files were processed successfully
     */
    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    /**
     * A relationship indicating an error while processing flow files
     */
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be processed")
            .build();


    static final PropertyDescriptor ROUTE_TO_FAILURE = new PropertyDescriptor.Builder()
            .name("Route to Failure")
            .description("If true, each FlowFile that is received will be routed to the failure relationship. If true, it will be routed to the success relationship.")
            .required(true)
            .allowableValues("odd", "even")
            .defaultValue("odd")
            .build();


    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private volatile int numFF = 0;

    /**
     * Returns the valid relationships for this processor.
     *
     * @return a Set of Relationships supported by this processor
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ROUTE_TO_FAILURE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        boolean routeToFailureOnOdd = context.getProperty(ROUTE_TO_FAILURE).getValue().equals("odd");
        boolean ffOdd = numFF % 2 == 1;
        session.transfer(flowFile, (routeToFailureOnOdd == ffOdd) ? REL_FAILURE : REL_SUCCESS);
        numFF++;
    }
}
