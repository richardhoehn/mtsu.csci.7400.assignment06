# import RDD
from pyspark.rdd import RDD

def printRdd(qNum, name, rdd):
    nameStr = ' ' + name + ' '
    question = 'Question ' + str(qNum) + ': '
    if(isinstance(rdd, RDD)):
        rddCollect = rdd.collect()
    else:
        rddCollect = rdd
    print('')
    print('*')
    print(question.ljust(30, '*'))
    print(nameStr.ljust(20, '*').rjust(30, '*'))
    print(rddCollect)
    print ('Type: ', type(rdd)) 
    print(nameStr.ljust(20, '*').rjust(30, '*'))
    print('*')
