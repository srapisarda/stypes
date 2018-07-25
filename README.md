**STypeS** 

Nonrecursive Datalog Rewriter for Linear TGDs and
Conjunctive Queries

STypeS algorithm is based on the article [Optimal Nonrecursive Datalog Rewritings of Linear TGDs and Bounded (Hyper)Tree-Width Queries](http://ceur-ws.org/Vol-1879/paper64.pdf)

STypeS rewrites ontology-mediated (OMQ) queries to equivalent
nonrecursive datalog queries (NDL) 

STypeS produces a polynomial-size rewritings whenever
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
* an ontology **O**, which is a set of linear tgds of the form :
**∀X γ(X) → ∃Y γ′(X′, Y)**, where **γ** and **γ′** are conjunction of atoms.  A tgd is linear if **γ(X)** is a single atom.
* a conjunctive query (CQ) **q**  of the form **∃y φ(x, y)**, where **φ** is a conjunction of atoms **P(z)** 
with predicate symbols from **Σ** and **z ⊆ x ∪ y**
* the CQ's tree decomposition **(T, λ)** in a standard GML form. 
A tree decomposition of a CQ **q** with variables **var(q)** is a pair **(T, λ)** of an (undirected) tree **T = (V, E)** and **λ: V → 2^[var(q)]** such that
  * for any atom **P(z) ∈ q**, there exists **v ∈ V** with **z ⊆ λ(v)**;
  * for any variable **z** in **q**, the set of vertices **{v ∈ V | z ∈ λ(v)}** is connected in **T**.



In order to execute the NDL-rewriting algorithm we need to use and execute 
the jar created above:  ***java -cp stypes-assembly-1.0.jar (CQ) (GML) (O)***

example:
```
cd ./target/scala-2.11/
java -cp stypes-assembly-1.0.jar uk.ac.bbk.dcs.stypes.App \
         ../../src/test/resources/q09.cq \
         ../../src/test/resources/q09.gml \
         ../../src/test/resources/lines.dlp

```

The conjunctive query [**q09.cq**](https://github.com/srapisarda/stypes/blob/master/src/test/resources/q09.cq)  which  has the form:
```
q(?x0, ?x9) <-  r(?x0, ?x1), r(?x1, ?x2), s(?x2, ?x3), r(?x3, ?x4), 
                s(?x4, ?x5), r(?x5, ?x6), s(?x6, ?x7), r(?x7, ?x8), 
                r(?x8, ?x9).
```

A possible **q09.cq**'s tree decomposition **(T, λ)**  is described in the file [**q09.gml**](https://github.com/srapisarda/stypes/blob/master/src/test/resources/q09.gml)

The ontology  [**lines.dlp**](https://github.com/srapisarda/stypes/blob/master/src/test/resources/lines.dlp)  contains is a set of 2 TGD's:
```
s(X,Y),r(Y,X) :- a(X).
r(X,Y),s(Y,X) :- b(X).
```
