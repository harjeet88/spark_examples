// AggregateByKey
// Used To aggregate values By Key and 
// Return values with different Type
// Need 3 Parameters
// 1. Initial Value
// 2. combine Function - To combine values with a partition
// 3. merge Function  - To combine values accross partitions

val in = Seq( "harry=78","raj=60" , "harry=75" , "harry=67" , "raj=89" , "raj=67" , "raj=72" ,"harry=72")

val data = sc.parallelize(in , 3)

val pair = data.map(x => x.split("=")).map(x => (x(0),x(1)))

val initial = 0 

val addOp = ( n:Int, v:String ) => n+v.toInt

val mergeOp = (p1:Int , p2: Int) => p1+p2

val out = pair.aggregateByKey(initial)( addOp, mergeOp)

out.collect.foreach(println)