Waterflow, a toy app vaguely inspired from Airflow as an excuse to practise basic scala stuff.

Technique to (try to) use:
* by name parameters with single argument (for the task)
* case class, enumeration, pattern matching
* basic sbt 
* type classes, given/using
* context bound in parametric functions or type
* Selectable 
* scala check
* opaque types
* repeated param list

* for comprehensions
* union types and intersection types

TODO:

* add several operators as `enum` with infix `>>` to chain them by building a tree. Test the operator with scalaCheck
* interpreter should start with a set of worker
* the `run()` method of hte runner demands that a `Runner[T]` exist for any reicived task `T`
* one of the task is initialized with a repeated param list (maybe it's just the list of name it print "hello" to or so)
* one of the task is accessing a Json DB (just an internal hashmap), use `Selectable` to parse various types into some case classes. Test the parsing with ScalaCheck
* from "config" (hard-coded case class), obtain the "speed" (controlling some Thread.sleep) and "capacity" (unused) config of each Worker. Use Opaque types for that.

Done:
* build a bare repo in Dropbox + checkout + document in Joplin
* `Dag { onePerator }` with a body build by-name, received by an interpreter which prints it