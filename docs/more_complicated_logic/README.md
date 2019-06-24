# A Pyspark use-case: creating features in a distributed environment

## Data Catalog
In data engineering, you create data pipelines frequently. This can range from the very small scope of a pyspark ETL job, to a series of transformation steps (like landing → clean → business) with possibly complex dependencies. E.g. a typical business use case will require you to combine multiple data sources. Because of this, you will typically want to have a _data catalog_ available: an object that you can query to access certain datasets in a structured way. It solves the problem of referring to specific paths all across user code by promoting one single truth.

One major advantage is that when a datalake (or even the older data warehouses) gets redesigned, all updates happen in one place. All ETL-code can stay the same, as each time the path to (non-homogeneous) data is requested, the catalog returns the correct definition. For batch jobs, tagging your datasets with an execution date and ensuring your pipelines take into account the foreseen execution date, will ensure you have easily rerunnable pipelines.

Exercise:
- implement a data catalog using a dictionary. Put the data catalog in its own module, as it is important enough by itself. In the catalog, refer to the following files:
  1. the flights data of the year 2000, which we've cleaned in the previous session.
  2. The [lookup table that maps IANA airport codes to their full names](https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_AIRPORT).
      3. The [lookup table that maps IATA carrier codes](https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_CARRIER_HISTORY) to airline carriers over a specific timerange.
  Ensure that your catalog defines the “raw” forms and the clean forms (which should have the correct datatypes and be stored in one of the 3 big-data formats, being either avro, parquet or orc.)
- Add to the data catalog an entry for a 360°-view of the flights data.
- Create a 360°-view of the flights data in which you combine the airline carriers (a dimension table), the airport names (another dimension table) and the flights tables (a facts table). Apply your `is_holiday` function to the date-column. 
You now have a dataset which can be used for further investigation, e.g. by a data scientist to see if there's a correlation between holidays and delays of flights.

At Data Minded, we've created [Lighthouse](https://github.com/datamindedbe/lighthouse), an open-source library using Spark at its core to create a catalog useful in big data contexts. Note that the library is designed for Scala. Due to popular demand, we're working on a Python port. It is not the highest priority though. Pieces of it get written in collaboration with clients with whom we do pyspark related development. That's a hint as well, in case you want to accelerate this process. 

### Pitfalls

**String joins**
In the previous (lengthy) exercise, you had to join two dataframes based on the equality of a string column.
One common source of problems with string joins is not using a **canonical form**. 
As an example, consider the Citroën car brand. Its official spelling is with the double-dots on the letter e. In many cases however, people will blindly type a join condition like this: `dimensions.carbrand == 'Citroën'`. For this to work as intended, there's a _silent assumption_ that the `carbrand` column has the same case and spelling as the word `Citroën`. In many situations, this is not the case: a developer often has no guarantees that data provided to him has been transformed into such a form. It is in these cases that a good developer will favor _correctness over efficiency_ and gladly call `get_canonical_form(dimensions.carbrand) == get_canonical_form('Citroën')`. And with this, we revisit the importance of _idempotence_: if the data had already been put into a canonical form, it is important that the function does not change the form when being reapplied. 

Exercise:
- write a string normalizer: remove leading and trailing whitespace, ensure there is a consistent case. You can safely assume you're only dealing with the ASCII character set, so you may ignore accented characters.
- (unit) test your string normalizer

One way to avoid string joins in the middle of an ETL pipeline is to normalize the data upon ingestion or when transforming landing zone data into a cleaned form. Especially with categorical variables, like province names, this works well. For people more familiar with databases, this is simply the process of normalizing data, using & creating _dimension tables_ as you go along.

**Cache everything**

When you create a master table by joining and transforming several (base) datasets, there will be times when a well-aimed `cache()` call on a Spark DataFrame can do wonders. The trick is to know when to apply it, because wen the memory gets full, caching will incur a performance hit: some data will need to be unpersisted or moved to the (much slower) hard drive (even if that is an SSD).

So when is it a good idea to call `cache`? Whenever you reuse a dataframe for a different purpose. To put it differently, anytime you can trace the lineage of two dataframes and find a common ancestor, it's at that point where the paths start diverging that you will want to cache.

Example:
```python
frame1 = frame0.filter(...).select(...).join(...)

frame2 = frame1.filter(..).distinct().groupBy(...).count()

frame3 = frame1.select(..).join(..).show()
```

In this example, `frame3` and `frame2` have a common ancestor: `frame1`. Because Spark DataFrames are evaluated lazily, the action that triggers the calculation of `frame3` will re-execute the transformations that lead to `frame1`. However, those same transformations have also been done when `frame2` was being calculated. In order to prevent the transformations that lead to `frame1` from being re-executed, `cache`-ing `frame1` is an excellent idea.
 
## Managing Workflows

Now that you have created a set of ETL jobs, it is time to orchestrate whenever each one should be run. You will want to automate this process, thereby freeing up your precious time for more useful things.

There exist several workflow management tools, some of which are open-source: 
* Azkaban (developed by LinkedIn) - Java based
* Luigi (developed by Spotify) - Python based
* Oozie - Java based
* Airflow (developed by Airbnb) - Python based

In this session, we will be looking into Apache Airflow, because of the ease with which pipelines can be written and scheduled. Moreover, it introduces an important devops concept: _configuration as code_.

### Configuration as Code

When configuration is written up as code, we obtain several benefits:
- machine failure is more easily overcome as code can simply be redeployed to new machines.
- environments can be made quasi identical with a single command. Imagine the number of clicks one would have to make with a graphical tool to get two environments set-up identically.
- code can be versioned. Hence, there is a historical log. It also typically lives close to the code that it is running.
- you can write code that creates processes dynamically!

That last piece is extremely powerful. Imagine you have some ETL job that is quasi identical for 20 files. The only difference would exist in one single parameter. Well, such scenarios are easily handled with (minor) scripting: one for-loop or a map construct and you've generated a bunch of processes that only differ in some small parameter(s).

## Apache-Airflow

Exercise:
- install apache-airflow

Question: where do you install it (dev or non-dev?)

>> EDIT: installing Apache Airflow during the training proved to be challenging on the Windows laptops. This should be tackled in the future when the laptops get pre-installed by the instructor. As we lost an hour or two trying to install it, we eventually decided just to showcase the possibilities of Airflow as a workflow management tool using the environment that the instructor had already set up on his laptop.

To the end-user, who is typically only interested in scheduling jobs at specified intervals, Apache Airflow has two important object-oriented notions: 
- that of a Directed Acyclic Graph (a DAG)
- and that of an Operator. An operator “does something”: it delegates a task to a new process, which could be the python interpreter, a unix shell, cloud services (like AWS batch), ...

You assign Operators to a certain DAG and then write out the dependencies between operators using either the bitshift operators (>> and <<), which have been overloaded for Operators in Airflow, or (if you like a more verbose style), using the `set_upstream` and `set_downstream` methods.

Exercices:
Up till this moment, each time you ran a job (a unit test, a Spark ETL job), you used your IDE (Pycharm) to run a Python process. If you paid close attention, you will have seen the exact command your IDE launched for you when you did this: it is the first line in your terminal window, the moment you press ctrl+shift+F10 (or similar).
Using the cleansing jobs that you have written earlier, and the master-view, all of which make use of a data catalog, write an Airflow DAG that runs these jobs in the correct sequence. Try to parallelize jobs that can be run in parallel. Make sure to also run this DAG. If you did it correctly, you will get to see output files appearing in the folders designated by your catalog.

While we are using Airflow only to demonstrate automated scheduling with complex dependencies, you can get a sense of its power and monitoring facilities by browsing the web interface of Airflow (run `airflow webserver` from the command line).


## Concourse CI/CD

A CI/CD tool is your silent member of the team. When instructed correctly, it will do many useful things with code you develop and infrastructure that you have scripted (-> configuration as code).

With Continuous Integration (CI), the goal is to integrate changes made by developers to "what is running in production" regularly. Multiple times per day is considered ideal. The thought behind this is that small changes can be easily inspected and do not require a lot of developer thought to understand. This "what is running in production" idea typically maps to the master branch of any version control system. Continuous Delivery (CD) is closely related: it ensures that the master branch is at all times ready to be deployed to production machines and users (think about documentation here).

We will be working with [Concourse](https://concourse-ci.org/).

Exercise:
- install Concourse. Note that Concourse is not to be installed using python.

>> EDIT: same comment as for Airflow. At KBC Jenkins is being used as a build tool. Some of the trainees have confirmed they know enough to work with it. 

