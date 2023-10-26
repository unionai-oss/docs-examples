# Spark Example

import datetime
import random
from operator import add

import flytekit
from flytekit import ImageSpec, Resources, task, workflow
from flytekitplugins.spark import Spark

# The Spark plugin contains an embedded Spark task container image.
# On workflow registration, that image is pushed to the container
# registry that you specify. From there it will be pulled at runtime and used to
# instantiate the Spark task container.
#
# To enable this, you must specify the coordinates of your container registry
# and the image name to be used.
#
# Make sure that the image is publicly accessible, so that the Union Cloud data plane
# can pull it.
spark_image = ImageSpec(registry="<registry_url>", name="<spark_image_name>")

# To define a Spark task, use a standard `@task` decorator and pass a `task_config` of type
# `flytekitplugins.spark.Spark`.
#
# You can then initialize the `flytekitplugins.spark.Spark` object itself using
# the `spark_conf`` parameter. This supports configuration settings commonly employed
# when setting up a Spark cluster. If necessary, you can also pass in
# a `hadoop_conf` parameter, which supports standard hadoop configuration settings.
#
# Directly within the `task_config` itself you can set the
# specify the resources to be allocated to the Spark task container just as you would in any other
# task using `limits` and `requests`.
#
# Finally, you must specify the `container_image` to be used for the Spark task container using
# the `ImageSpec` object defined above.
#
# The `hello_spark` task initiates a new Spark cluster-within-a-cluster.
# When executed locally, it sets up a single-node client-only cluster.
# When executed remotely, it dynamically scales the cluster size based on
# the specified Spark configuration.
#
# For this particular example, we define a function `f()` upon which the
# map-reduce operation is invoked within the Spark cluster.
@task(
    task_config=Spark(
        spark_conf={
            "spark.driver.memory": "1000M",
            "spark.executor.memory": "1000M",
            "spark.executor.cores": "1",
            "spark.executor.instances": "2",
            "spark.driver.cores": "1",
        }
    ),
    limits=Resources(mem="2000M"),
    container_image=spark_image,
)
def hello_spark(partitions: int) -> float:
    print("Starting Spark with Partitions: {}".format(partitions))
    n = 1 * partitions
    sess = flytekit.current_context().spark_session
    count = sess.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi_val = 4.0 * count / n
    return pi_val


# The function `f()` used by the map-reduce operation in the Spark task.def f(_):
    x = random.random() * 2 - 1
    y = random.random() * 2 - 1
    return 1 if x**2 + y**2 <= 1 else 0


# For demonstration purposes, we specify a standard task that won't be executed on the Spark cluster.
@task(cache_version="2")
def print_every_time(value_to_print: float, date_triggered: datetime.datetime) -> int:
    print("My printed value: {} @ {}".format(value_to_print, date_triggered))
    return 1


# A workflow that connects the tasks in a sequence.
# Spark and non-Spark tasks can be chained together as long as their parameter specifications match.
@workflow
def my_spark(triggered_date: datetime.datetime = datetime.datetime.now()) -> float:
    pi = hello_spark(partitions=1)
    print_every_time(value_to_print=pi, date_triggered=triggered_date)
    return pi
