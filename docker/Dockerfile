FROM stackbrew/ubuntu:14.10
RUN apt-get update
RUN apt-get install -y git

RUN sed -i 's/main$/main universe/' /etc/apt/sources.list
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y -q fakeroot
RUN DEBIAN_FRONTEND=noninteractive fakeroot apt-get install -y -q openjdk-7-jdk
RUN DEBIAN_FRONTEND=noninteractive fakeroot apt-get install -y -q maven

ADD http://apache.proserve.nl/jena/binaries/jena-fuseki-1.1.1-distribution.tar.gz  /tmp/fuseki.tar.gz
RUN tar -xzf /tmp/fuseki.tar.gz -C /usr/share

RUN git clone https://github.com/connorstroomberg/jena-es 

ENV LC_ALL en_US.UTF-8

RUN mvn -f jena-es/pom.xml install

ENV FUSEKI_HOME /usr/share/jena-fuseki-1.1.1
ENV PATH $FUSEKI_HOME:$PATH
RUN mkdir /DB
ADD depression.trig .
ADD hypertension.trig .
ADD documents.trig .
RUN java -Xmx1200M -cp jena-es/target/JenaEventSourcing-0.0.1-SNAPSHOT-jar-with-dependencies.jar util.InsertData --desc jena-es/assemble-es.ttl "depression.trig"
RUN java -Xmx1200M -cp jena-es/target/JenaEventSourcing-0.0.1-SNAPSHOT-jar-with-dependencies.jar util.InsertData --desc jena-es/assemble-es.ttl "hypertension.trig"
RUN java -Xmx1200M -cp jena-es/target/JenaEventSourcing-0.0.1-SNAPSHOT-jar-with-dependencies.jar util.InsertData --desc jena-es/assemble-es.ttl "documents.trig"

EXPOSE 3030
CMD ["java", "-Xmx1200M", "-jar", "jena-es/target/JenaEventSourcing-0.0.1-SNAPSHOT-jar-with-dependencies.jar", "--pages", "/usr/share/jena-fuseki-1.1.1/pages", "--port", "3030", "--conf", "jena-es/assemble-es.ttl", "--update"]