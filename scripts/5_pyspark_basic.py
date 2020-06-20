


# pyspark basic 

# from python array to rdd
rdd = sc.parallelize([{"cat", 1}, {"dog", 1}, {"cat", 2}])
rdd.take(10)


# read a text file
logs = sc.textFile("/var/log/mongodb/*");
logs.count()

# map
rdd = sc.parallelize([1, 2, 3, 4, 5])
new_rdd = rdd.map(lambda x: (x + 2))
new_rdd.collect()
# [3, 4, 5, 6, 7]

# flat map
new_rdd = rdd.flatMap(lambda x: (x - 1, x, x + 1))
new_rdd.collect()
# [0, 1, 2, 1, 2, 3, 2, 3, 4, 3, 4, 5, 4, 5, 6]

# filter
new_rdd = rdd.filter(lambda x: x > 3)
new_rdd.collect()
# [4, 5]



# reduce by key
rdd = sc.parallelize([("cat", 1), ("dog", 1), ("cat", 2)])
rdd.reduceByKey(lambda x, y: (x + y)).collect()
# [('cat', 3), ('dog' ,1)]


# group by key
rdd.groupByKey().collect()
rdd.groupByKey().mapValues(list).collect()
# [('cat', [1, 2]), ('dog', [1])]


# join
people = sc.parallelize([(1, "Mauro"), (2, "Paola"), (3, "Claudia"), (4, "Mario")])
age = sc.parallelize([(1, 36), (2, 30), (4, 71)])
rdd = people.join(age)
rdd.collect()
# [(2, ('Paola', 30)), (4, ('Mario', 71)), (1, ('Mauro', 36))]

# left join
rdd1 = people.leftOuterJoin(age)
rdd1.collect()
# [(2, ('Paola', 30)), (4, ('Mario', 71)), (1, ('Mauro', 36)), (3, ('Claudia', None))]

# filter
people = sc.parallelize([(1, "Mauro"), (2, "Paola"), (3, "Claudia"), (4, "Mario")])
age = sc.parallelize([(1, 36), (2, 30), (4, 71)])
rdd1 = people.leftOuterJoin(age)
rdd1.filter(lambda x: x[1][1] is not None and x[1][1] > 30).collect()
# [(4, ('Mario', 71)), (1, ('Mauro', 36))]


# take
people = sc.parallelize([(1, "Mauro"), (2, "Paola"), (3, "Claudia"), (4, "Mario")])
age = sc.parallelize([(1, 36), (2, 30), (4, 71)])
rdd1 = people.leftOuterJoin(age)
rdd1_filter = rdd1.filter(lambda x: x[1][1] is not None and x[1][1] > 30)
rdd1.take(2)
# [(4, ('Mario', 71)), (1, ('Mauro', 36))]


# count
people = sc.parallelize([(1, "Mauro"), (2, "Paola"), (3, "Claudia"), (4, "Mario")])
age = sc.parallelize([(1, 36), (2, 30), (4, 71)])
rdd1 = people.leftOuterJoin(age)
rdd1.count()
# 4



# age average
people = sc.parallelize([(1, "Mauro"), (2, "Paola"), (3, "Claudia"), (4, "Mario")])
age = sc.parallelize([(1, 36), (2, 30), (4, 71)])
rdd = people.join(age)


### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????
### ???????


# age average
people = sc.parallelize([(1, "Mauro"), (2, "Paola"), (3, "Claudia"), (4, "Mario")])
age = sc.parallelize([(1, 36), (2, 30), (4, 71)])
rdd = people.join(age)

rdd.map(lambda x: x[1][1]).reduce(lambda x, y: x + y) / rdd.count()
# 45.66666666666

rdd.map(lambda x: x[1][1]).sum() / rdd.count()
rdd.map(lambda x: x[1][1]).mean()



# spark sql
data = spark.read.option("header", "true").option("delimiter",";").csv("/home/master/dataset/BookCrossing/BX-CSV-Master/BX-Book-Ratings.csv")

data.printSchema()
data.count()
data.show()
data.createOrReplaceTempView("ratings")

users = spark.read.option("header", "true").option("delimiter",";").csv("/home/master/dataset/BookCrossing/BX-CSV-Master/BX-Users.csv")
users.count()
users.show()
users.createOrReplaceTempView("users")

dx = spark.sql("select * from users where Age is not null")
dy = dx.dropDuplicates()
dy.count()

