from  loadenv import SparkEnv
(se, sc) = SparkEnv("lesson_1").postInit()
lines = sc.textFile(se.projLoc() + r"\input\test.data")

def class1():
    pythonLines = lines.filter(lambda line: "choice" in line)
    pythonLines.persist()
    print(pythonLines.count())
    print(pythonLines.first())

def class2():
    # filter
    rdd1 = lines.filter(lambda x: "mouse" in x)
    # union
    rdd2 = lines.filter(lambda x: "choice" in x)
    rdd3 = rdd1.union(rdd2)
    print(rdd1.count())
    print(rdd2.count())
    print(rdd3.count())

def class3():
    print ("Input had ", lines.count() , " concerning lines")
    print ("Here are 10 examples:")
    for line in lines.take(10):
        print (line)

def containsI(s):
        return "I" in s

def class4():
    word = lines.filter(lambda s: "I" in s)
    print("lambda过滤后：" , word.count())
    word = lines.filter(containsI)
    print("传入函数过滤后：" , word.count())

# 求平方和
def class5():
    nums = sc.parallelize([1, 2, 3, 4])
    squared = nums.map(lambda x: x * x).collect()
    for num in squared:
        print ("%i " % (num))

# flatMap
def class6():
    lines = sc.parallelize(["hello world", "hi"])
    words = lines.flatMap(lambda line: line.split(" "))
    words.first()

# 求和
def class7():
    nums = sc.parallelize([1, 2, 3, 4])
    sum = nums.reduce(lambda x, y: x + y)
    print(sum)

# 计算平均值
def class8():
    nums = sc.parallelize([[1, 2], [1, 2], [1, 2], [1, 2]])
    sumCount = nums.aggregate((0, 0), (lambda acc, value: (acc[0] + value, acc[1] + 1), (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))))
    return sumCount[0] / float(sumCount[1])

if __name__ == "__main__":
    class8();
