# Spark on Union Cloud

## Introduction

Union Cloud supports the native execution of Spark jobs without the need for an external service. This functionality leverages the open-sourced [spark-on-k8s-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/tree/master) to run a transient Spark cluster within your existing Union Cloud dataplane cluster. The transient cluster-within-a-cluster is spun up for a specific Spark job and torn down after completion. Union Cloud automatically manages this lifecycle.

Your Spark code is composed using PySpark syntax in the body of a specialized Flyte task. The task declaration includes configuration that controls aspects of the transient Spark cluster.

The examples provided in this section offer a hands-on tutorial for writing PySpark tasks.


## Why use Spark on Union Cloud?

Managing Python dependencies can be challenging, but Union Cloud simplifies the process by enabling easy versioning and management of dependencies through containers.
Spark on Union Cloud then extends the benefits of this containerization to Spark without requiring the management of specialized Spark clusters.

Pros:

1. Simple to get started, providing complete isolation between workloads.
2. Each job runs in isolation with its own virtual cluster, eliminating the complexities of dependency management.
3. Flyte takes care of all the management tasks.

Cons:

1. Short-running, bursty jobs may not be the best fit due to container overhead.
2. Interactive Spark capabilities are not available with Flyte Kubernetes Dask;
   instead, it is better suited for running adhoc and scheduled jobs.


## Set up your environment

To run this example, you will need to install the following Python packages:

* `flytekit`: The standard Flytekit package.
* `flytekitplugins-spark`: The Spark plugin package.
* `flytekitplugins-envd`: The environment Docker image builder plugin package needed to support `ImageSpec`.

You should set up a Python virtual environment and install these packages in that environment. The easiest way to do this is to use the provided `requirements.txt` file:

```shell
pip install -r requirements.txt
```

::: details Note

Ensure that your Union Cloud dataplane cluster has sufficient resources available.
Depending on the resource requirements of your Spark job, you may need to adjust the resource quotas for the namespace accordingly.

:::


## Build the container image


## Run the example on Union Cloud

To run the provided example on Union CLoud, use any of the following commands:

```
pyflyte run --remote \
  https://raw.githubusercontent.com/flyteorg/flytesnacks/master/examples/k8s_spark_plugin/k8s_spark_plugin/pyspark_pi.py \
  my_spark
```

```
pyflyte run --remote \
  https://raw.githubusercontent.com/flyteorg/flytesnacks/master/examples/k8s_spark_plugin/k8s_spark_plugin/dataframe_passing.py \
  my_smart_structured_dataset
```

(spark-examples)=

```{auto-examples-toc}
pyspark_pi
dataframe_passing
```








::: details Note

Running Spark jobs within Union Cloud has the advantage that the cost is amortized because pods are faster to create than on a separate machine or external service. However, the penalty of downloading Docker images may affect the performance.

Also, remember that starting a pod is not as fast as running a process.

This plugin has been tested at scale, and more than 100k Spark Jobs run through Flyte at Lyft.

For optimal results, we highly recommend adopting the
[multi-cluster mode](https://docs.flyte.org/en/latest/deployment/configuration/performance.html#multi-cluster-mode).
Additionally, consider enabling {std:ref}`resource quotas <deployment/configuration/general:configurable resource types>`
for Spark Jobs that are both large in scale and executed frequently.

This still needs a large capacity on Kubernetes and careful configuration. We recommend using Multi-cluster mode - deployment/cluster_config/performance:multi-cluster mode , and enabling Resource Quotas for large and extremely frequent Spark Jobs. This is not recommended for extremely short-running jobs, and it might be better to use a pre-spawned cluster. A job can be considered short if the runtime is less than 2-3 minutes. In this scenario, the cost of pod bring-up outweighs the cost of execution.

This plugin has been rigorously tested at scale, successfully managing more than 100,000 Spark Jobs through Flyte at Lyft.
However, please bear in mind that this functionality requires a significant Kubernetes capacity and meticulous configuration.



Nonetheless, it is important to note that extremely short-duration jobs might not be the best fit for this setup.
In such cases, utilizing a pre-spawned cluster could be more advantageous.
A job can be considered "short" if its runtime is less than 2 to 3 minutes.
In these situations, the cost of initializing pods might outweigh the actual execution cost.

:::
