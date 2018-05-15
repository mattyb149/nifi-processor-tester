# nifi-processor-tester
A project to create a stub/mock environment for testing NiFi processors.

## Usage

java -cp nifi-processor-tester-<version>-all.jar [options] script_file

  Where options may include:
  
    -content            Output flow file contents. Defaults to false
    
    -attrs              Output flow file attributes. Defaults to false
    
    -all                Output content, attributes, etc. about flow files that were transferred to any relationship. Defaults to false
    
    -input=<directory>  Send each file in the specified directory as a flow file to the script
    
    -nifi-path=<path>   Path to folder containing the NAR with processor under test, and any parent NARs

    -attrfile=<path>    Path to a properties file specifying attributes to add to incoming flow files
    
    
    
## Build

To build the fat JAR, just run the following command:

```gradle
gradle shadowJar
```


## Download
The JAR is available on Bintray at https://bintray.com/mattyb149/maven/nifi-processor-tester


### Maven
```maven
<dependency>
  <groupId>mattyb149</groupId>
  <artifactId>nifi-processor-tester</artifactId>
  <version>1.0.0.1.7.0</version>
  <type>jar</type>
  <classifier>all</classifier>
</dependency>
```

### Gradle
```gradle
compile(group: 'mattyb149', name: 'nifi-processor-tester', version: '1.0.0.1.7.0', ext: 'jar', classifier: 'all')
```

## License

nifi-processor-tester is copyright 2018- Matthew Burgess except where otherwise noted.

This project is licensed under the Apache License Version 2.0 except where otherwise noted in the source files.
