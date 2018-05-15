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

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * The main entry class for testing ExecuteScript
 */
public class Runner {

    public static String DASHED_LINE = "---------------------------------------------------------";

    private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);
    private static final String KEY_FILE_FLAG = "-K";
    private static TestRunner runner;

    private static boolean outputAttributes = false;
    private static boolean outputContent = false;
    private static String inputFileDir = "";
    private static String procName = "";
    private static String narPath = "";
    private static String attrFile = "";
    private static int numFiles = 0;

    private static void printUsage() {
        System.err.println("Usage: java -jar nifi-proc-tester-<version>-all.jar [options] processor_class_simple_name");
        System.err.println(" Where options may include:");
        System.err.println("   -content            Output flow file contents. Defaults to false");
        System.err.println("   -attrs              Output flow file attributes. Defaults to false");
        System.err.println("   -all                Output content, attributes, etc. about flow files that were transferred to any relationship. Defaults to false");
        System.err.println("   -input=<directory>  Send each file in the specified directory as a flow file to the script");
        System.err.println("   -nifi-path=<path>   Path to folder containing the NAR with processor under test, and any parent NARs");
        System.err.println("   -attrfile=<paths>   Path to a properties file specifying attributes to add to incoming flow files.");
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        // Expecting a single arg with the filename, will figure out language from file extension
        if (args == null || args.length < 1) {
            printUsage();
            System.exit(1);
        }

        // Reset option flags
        outputAttributes = false;
        outputContent = false;
        procName = "";
        inputFileDir = "";
        attrFile = "";
        numFiles = 0;

        for (String arg : args) {
            if ("-all".equals(arg)) {
                outputAttributes = true;
                outputContent = true;
            } else if ("-content".equals(arg)) {
                outputContent = true;
            } else if ("-attrs".equals(arg)) {
                outputAttributes = true;
            } else if (arg.startsWith("-input=")) {
                inputFileDir = arg.substring("-input=".length());
            } else if (arg.startsWith("-nar-path=")) {
                narPath = arg.substring("-nar-path=".length());
            } else if (arg.startsWith("-attrfile=")) {
                attrFile = arg.substring("-attrfile=".length());
            } else {
                procName = arg;
            }
        }

        if ("".equals(procName)) {
            System.err.println("No processor specified");
            printUsage();
            System.exit(1);
        }

        try {
            //final ClassLoader bootstrap = createBootstrapClassLoader(nifiPropsPath, narPath);
            Properties props = new Properties();
            props.put(NiFiProperties.NAR_WORKING_DIRECTORY, NiFiProperties.DEFAULT_NAR_WORKING_DIR);
            props.put(NiFiProperties.NAR_LIBRARY_DIRECTORY, narPath);

            NiFiProperties properties = new StandardNiFiProperties(props);
            properties.validate();
            final Bundle systemBundle = SystemBundle.create(properties);

            // expand the nars
            NarUnpacker.unpackNars(properties, systemBundle);

            // load the extensions classloaders
            NarClassLoaders narClassLoaders = NarClassLoaders.getInstance();


            narClassLoaders.init(Paths.get(narPath, properties.getFrameworkWorkingDirectory().toString()).toFile(),
                    Paths.get(narPath, properties.getExtensionsWorkingDirectory().toString()).toFile());

            ExtensionManager.discoverExtensions(systemBundle, narClassLoaders.getBundles());
        } catch (final Throwable t) {
            LOGGER.error("Failure to launch NiFi due to " + t, t);
            t.printStackTrace();
        }


        Class<? extends Processor> processor;
        Optional<Class> proc = ExtensionManager.getExtensions(Processor.class).stream().filter(p -> p.getSimpleName().equals(procName)).findFirst();
        if (!proc.isPresent()) {
            System.out.println("Could not find processor class");
            System.exit(11);
        }
        processor = (Class<? extends Processor>) proc.get();

        Processor procInstance = null;
        try {
            procInstance = processor.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            System.exit(20);
        }

        runner = TestRunners.newTestRunner(procInstance);
        runner.assertValid();

        // Get incoming attributes from file (if specified)
        Map<String, String> incomingAttributes = new HashMap<>();
        Path attrFilePath = Paths.get(attrFile);
        if (!attrFile.isEmpty()) {
            if (!Files.exists(attrFilePath)) {
                System.err.println("Attribute file does not exist: " + attrFile);
                System.exit(5);
            } else {
                try {
                    Properties p = new Properties();
                    p.load(Files.newBufferedReader(attrFilePath));
                    p.forEach((k, v) -> incomingAttributes.put(k.toString(), v.toString()));
                } catch (IOException ioe) {
                    System.err.println("Could not read properties file: " + attrFile + ", reason: " + ioe.getLocalizedMessage());
                    System.exit(5);
                }
            }
        }

        try {
            if (inputFileDir.isEmpty()) {
                int available = System.in.available();
                if (available > 0) {
                    InputStreamReader isr = new InputStreamReader(System.in);
                    char[] input = new char[available];
                    isr.read(input);
                    runner.enqueue(new String(input), incomingAttributes);
                }
            } else {
                // Read flow files in from the folder
                Path inputFiles = Paths.get(inputFileDir);
                if (!Files.exists(inputFiles)) {
                    System.err.println("Input file directory does not exist: " + inputFileDir);
                    System.exit(3);
                }
                if (!Files.isDirectory(inputFiles)) {
                    System.err.println("Input file location is not a directory: " + inputFileDir);
                    System.exit(4);
                }
                Files.walkFileTree(inputFiles, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (attrs.isRegularFile()) {
                            incomingAttributes.put("filename", file.getFileName().toString());
                            runner.enqueue(Files.readAllBytes(file), incomingAttributes);
                            numFiles++;
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        if (numFiles > 1) {
            runner.run(numFiles);
        } else {
            runner.run();
        }
        // TODO output
        Set<Relationship> rels = procInstance.getRelationships();
        rels.forEach(Runner::outputFlowFilesForRelationship);

    }

    private static void outputFlowFilesForRelationship(Relationship relationship) {

        List<MockFlowFile> files = runner.getFlowFilesForRelationship(relationship);
        if (files != null) {
            for (MockFlowFile flowFile : files) {
                if (outputAttributes) {
                    final StringBuilder message = new StringBuilder();
                    message.append("Flow file ").append(flowFile);
                    message.append("\n");
                    message.append(DASHED_LINE);
                    message.append("\nFlowFile Attributes");
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "entryDate", new Date(flowFile.getEntryDate())));
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "lineageStartDate", new Date(flowFile.getLineageStartDate())));
                    message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", "fileSize", flowFile.getSize()));
                    message.append("\nFlowFile Attribute Map Content");
                    for (final String key : flowFile.getAttributes().keySet()) {
                        message.append(String.format("\nKey: '%1$s'\n\tValue: '%2$s'", key, flowFile.getAttribute(key)));
                    }
                    message.append("\n");
                    message.append(DASHED_LINE);
                    System.out.println(message.toString());
                }
                if (outputContent) {
                    System.out.println(new String(flowFile.toByteArray()));
                }
                System.out.println("");
            }
            System.out.println("Flow Files transferred to " + relationship.getName() + ": " + files.size() + "\n");
        }
    }

    private static ClassLoader createBootstrapClassLoader(String basePath, String narPath) throws IOException {
        //Get list of files in bootstrap folder
        final List<URL> urls = new ArrayList<>();
        Files.list(Paths.get(basePath, "lib/bootstrap")).forEach(p -> {
            try {
                urls.add(p.toUri().toURL());
            } catch (final MalformedURLException mef) {
                LOGGER.warn("Unable to load " + p.getFileName() + " due to " + mef, mef);
            }
        });

        // Get list of JARs in lib folder (in regular NiFi this is done by RunNiFi as the initial classpath)
        Files.list(Paths.get(basePath, "lib")).filter(f -> f.toString().endsWith(".jar")).forEach(p -> {
            try {
                urls.add(p.toUri().toURL());
            } catch (final MalformedURLException mef) {
                LOGGER.warn("Unable to load " + p.getFileName() + " due to " + mef, mef);
            }
        });

        //Create the bootstrap classloader
        return new URLClassLoader(urls.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
    }
}

