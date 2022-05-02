# Flink Code Exercise
A quick start exercise for understand the implement for different flink API with a window agg action
Such as SQL, Window agg, Window Apply and ProcessFunction

![](levels_of_abstraction.svg)

In the file of src/main/java/com/flinkpd/ProcessfunctionDemo.java has implemented a window aggregate action.
please understand the logic and re-code under window.apply(new RichWindowFunction), window.%agg() and SQL, respectively.


* JDK 1.8
* Flink version 1.14.0
* Scala version 2.11

PS: Because SQL job need additional dependenies, Only DML is necessary.

good luck

[1] https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/concepts/overview/
