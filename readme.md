Waterflow, a toy app vaguely inspired from Airflow as an excuse to practise basic scala stuff.

# Goal

Find an excuse for using the following Scala techniques:
* enums of case classes
* exports
* scalatest with property-based testing
* tail recursion

* type classes, given/using
* context bound in parametric functions or type
* Selectable 
* opaque types
* union types and intersection types

# TODO-list

## Done
* Enable a user to build a DAG with one single Task
* Add an `>>` operator to declare dependencies between tasks
* Build a DAG with `Dag.apply(List[Task])`, resolving all dependencies and checking for cycles
* Add scalatest/scalacheck UT for the DAG validation logic
* Add a basic scheduler

## Ongoing

## Todo
* Type class and context bound: the `run()` method of any worker demands that a `Runner[T]` exist for any received task `T`
* Selectable: one of the task is accessing a Json DB (just an internal hashmap), use `Selectable` to parse various types into some case classes.
* Opaque types: obtain the "speed" (controlling some Thread.sleep) and "capacity" (unused) config of each Worker from "config" (hard-coded case class). Config params are implemented with opaque types
* union/intersection types: add possibility to customize Task by stacking traits on to them. Use pattern matching with union/intersection types in the Runner to react to that customization

