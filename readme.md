Waterflow, a toy app vaguely inspired from Airflow as an excuse to practise basic scala stuff.

# Goal

Find an excuse for using the following Scala techniques:

* enums of case classes
* exports
* type classes, given/using
* context bound in parametric functions or type
* Selectable 
* scala check property-based tests
* opaque types
* union types and intersection types

DONE:
* project scaffolding
* allow a user to build a `Dag { oneTask }` with a body build by-name.
* add an `>>` operator to declare dependencies between task
* build a dag with `Dag.apply(List[Task])`, building a lineage out of the dependencies.

ONGOING:
* Add scalatest/scalacheck UT + make sure indepentent tasks are handled correctly: Noop task must be ignored

TODO:
* start a set of "worker" from the interpreter, to which the tasks are dispatched
* Type class and context bound: the `run()` method of any worker demands that a `Runner[T]` exist for any received task `T`
* Selectable: one of the task is accessing a Json DB (just an internal hashmap), use `Selectable` to parse various types into some case classes.
* Opaque types: obtain the "speed" (controlling some Thread.sleep) and "capacity" (unused) config of each Worker from "config" (hard-coded case class). Config params are implemented with opaque types
* union/intersection types: add possibility to customize Task by stacking traits on to them. Use pattern matching with union/intersection types in worker to react to that customization

