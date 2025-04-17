---
id: uuid
title: "Assigning Universal IDs"
hide_title: true
hide_table_of_contents: true
sidebar_position: 1
---

# Assigning Universal IDs

In the previous section we saw a high-level overview of what an EMPI software system is
and of how Tuva EMPI works. One of the key steps in the Tuva EMPI data flow
is the assignment of universal IDs to all records (step 4 in the previous section).
In this section we drill into that process to offer more detail of how
universal patient identifiers are created by Tuva EMPI.

There are 3 key components required to assign universal patient identifiers
to patient records:

#### 1) Record Matching

The first step necessary to assign universal patient identifiers to all patient
records is to do pairwise comparisons of records and determine the probability
that any given pair of records corresponds to the same real-world person.
We need to create a function _F_ that takes two patient records as inputs and
returns the probability that those two records correspond to the same real-world person:

_F(R1, R2) = P_same_person_

Where _R1_ and _R2_ are two distinct patient records and _P_same_person_
is the probability that _R1_ and _R2_ correspond to the same real-world person.
We could use the function _F_ to compare every possible pair of records in
our raw data and log the probabilities that each possible pair corresponds to
the same real-world person, but that would result in _N(N-1)/2_ comparisons,
which grows as the square of the number of records in your dataset and
results in a prohibitively large number of comparisons for large datasets.
As we'll see later, get around this problem by defining so-called "blocking rules" to limit
the number of comparisons made.
