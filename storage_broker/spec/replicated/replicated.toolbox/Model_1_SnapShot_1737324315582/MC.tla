---- MODULE MC ----
EXTENDS replicated, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
b1, b2, b3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
s1, s2, s3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
p1
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
az1, az2, az3
----

\* MV CONSTANT definitions brokers
const_1737324311514327000 == 
{b1, b2, b3}
----

\* MV CONSTANT definitions safekeepers
const_1737324311514328000 == 
{s1, s2, s3}
----

\* MV CONSTANT definitions pageservers
const_1737324311514329000 == 
{p1}
----

\* MV CONSTANT definitions azs
const_1737324311514330000 == 
{az1, az2, az3}
----

\* SYMMETRY definition
symm_1737324311514331000 == 
Permutations(const_1737324311514327000) \union Permutations(const_1737324311514328000) \union Permutations(const_1737324311514329000) \union Permutations(const_1737324311514330000)
----

\* CONSTANT definitions @modelParameterConstants:4max_commit_lsn
const_1737324311514332000 == 
2
----

\* CONSTANT definitions @modelParameterConstants:5az_mapping
const_1737324311514333000 == 
[ az1 |-> {b1,s1,p1} , az2 |-> {b2,s2} , az3 |-> {b3,s3}]
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_1737324311514334000 ==
StateConstraint
----
=============================================================================
\* Modification History
\* Created Sun Jan 19 23:05:11 CET 2025 by cs
