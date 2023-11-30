# ML4all: scalable ML system for everyone


ML4all is a system that frees users from the burden of machine learning algorithm selection and low-level implementation details.
It uses a new abstraction that is capable of solving most ML tasks and provides a cost-based optimizer on top of the proposed abstraction for choosing the best gradient descent algorithm in a given setting.
Our results show that ML4all is more than two orders of magnitude faster than state-of-the-art systems and can process large datasets that were not possible before.

More details can be found in our dedicated [SIGMOD publication](https://dl.acm.org/citation.cfm?id=3064042) and 
in Wayang's core [system paper](https://sigmodrecord.org/publications/sigmodRecord/2309/pdfs/05_Systems_Beedkar.pdf).

## Abstraction
ML4all abstracts most ML algorithms with seven operators:

- (1) `Transform` receives a data point to transform
(e.g., normalize it) and outputs a new data point.

- (2) `Stage` initializes all the required global param-
eters (e.g., centroids for the k-means algorithm).

- (3) `Compute` performs user-defined computations
on the input data point and returns a new data
point. For example, it can compute the nearest cen-
troid for each input data point.

- (4) `Update` updates the global parameters based on
a user-defined formula. For example, it can update
the new centroids based on the output computed by
the Compute operator.

- (5) `Sample` takes as input the size of the desired
sample and the data points to sample from and re-
turns a reduced set of sampled data points.

- (6) `Converge` specifies a function that outputs
a convergence dataset required for determining
whether the iterations should continue or stop.

- (7) `Loop` specifies the stopping condition on the
convergence dataset.

Similar to MapReduce, where users need to implement a map and reduce function, users of ML4all wishing to develop their own algorithm should implement the above interfaces.
The interfaces can be found in `org.apache.wayang.ml4all.abstraction.api`.

Examples for KMeans clustering and stochastic gradient descent can be found in `org.apache.wayang.ml4all.algorithms`.

## Example runs
- Kmeans:

```shell
./bin/wayang-submit org.apache.wayang.ml4all.examples.RunKMeans java,spark <url_path_to_file>/USCensus1990-sample.input 3 68 0 1
```

- SGD:
```shell
./bin/wayang-submit org.apache.wayang.ml4all.examples.RunSGD spark <url_path_to_file>/adult.zeros.input 100827 123 10 0.001
```
