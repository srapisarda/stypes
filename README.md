**STypeS** 

Nonrecursive Datalog Rewriter for Linear TGDs and
Conjunctive Queries

STYPES rewrites ontology-mediated (OMQ) queries to equivalent
nonrecursive datalog queries (NDL) 

STYPES produces a polynomial-size rewritings whenever
the treewidth of the input CQs and the size of the chase
for the ontology atoms are bounded; moreover, the rewriting can be
constructed and executed in LogCFL, which is optimal in order 
to be evaluated in an high parallelizable environments as Apache Flink.

In order to execute  STypeS in local environment, 
it is necessary to build the application by including all the 
dependencies. 
From terminal command line execute the following:

```
git clone https://github.com/srapisarda/stypes.git
cd stypes
sbt 'set test in assembly := {}' clean assembly 
```

The command  above  will create a jar file in "./target/scala-2.11/stypes_2.11-1.0.jar"

STypeS takes three inputs:
* an ontology O, which is a set of linear tgds
* a conjunctive query CQ 
* the CQ's tree decomposition (T, λ) in a standard GML form.

In order to execute the NDL-rewriting algorithm we need to use and execute 
the jar created above:  java -cp stypes-assembly-1.0.jar <CQ> <GML> <O>

example:
```
cd ./target/scala-2.11/
java -cp stypes-assembly-1.0.jar uk.ac.bbk.dcs.stypes.App \
         ../../src/test/resources/q09.cq \
         ../../src/test/resources/q09.gml \
         ../../src/test/resources/lines.dlp

```


