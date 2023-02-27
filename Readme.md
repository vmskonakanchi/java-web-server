#### Command to compile

``mvn clean compile assembly:single``

#### Need to install webserver.jar locally using below command

``mvn install:install-file -Dfile=webserver.jar -DgroupId=com.webserver -DartifactId=webserver -Dversion=1.0 -Dpackaging=jar``