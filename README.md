scalding-taps
=============

Series of Taps for Scalding

At the moment only the Tap for ElasticSearch is available but we are planning to develop a similar one for SolR and for other system we need to connect to.


Using Scalding Taps
===================

Scalding-taps is currently at version 0.2 and is published on the Conjars repository

To add it to your project you just have to add the reference to Conjars repository and the dependency to io.scalding - scalding-taps_2.10 - 0.2
As an example for Maven

```
     ...

     <repositories>
        <repository>
            <id>conjars.org</id>
            <url>http://conjars.org/repo</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>io.scalding</groupId>
            <artifactId>scalding-taps_2.10</artifactId>
            <version>0.2</version>
        </dependency>

    ...

    </dependencies>
```

