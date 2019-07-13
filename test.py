from pyspark import SparkContext
import jieba.posseg as pseg
import jieba
import re
import datetime

def split_line(line):
    dict_ = open("people_name_list.txt")
    dict_words = dict_.readlines()
    for word in dict_words:
        word = ''.join(re.findall(r'[\u4e00-\u9fa5]', word))
        jieba.add_word(word, tag="my_name")
    dict_.close()
    line = ''.join(re.findall(r'[\u4e00-\u9fa5]', line))
    words = pseg.lcut(line)
    nameStr = ''
    for word in words:
        if word.flag == "my_name":
            nameStr = nameStr + ' ' + word.word
    nameStr = nameStr.strip()
    return nameStr

def split_links(url):
    full_link = url[1][0]
    father_rank = url[1][1]
    length = int(len(full_link)/2)
    result = []
    for i in range(length):
        sub_link = full_link[2 * i]
        weight = full_link[2 * i + 1]
        cur_rank = father_rank * weight
        result.append((sub_link, cur_rank))
    return result

def distinct_name(words):
    myList = []
    for word in words:
        if len(word) > 0 and word not in myList:
            myList.append(word)
    return myList


def generateRelation(names):
    length = len(names)
    myList = []
    for i in range(0, length):
        for j in range(i+1, length):
            strKey1 = '' + names[i] + ' ' + names[j]
            strKey2 = '' + names[j] + ' ' + names[i]
            myList.append((strKey1, 1))
            myList.append((strKey2, 1))
    return myList


def generateNet(turples):
    relation = turples[0].split(' ')
    return (relation[0], [relation[1], turples[1]])


def mergeList(a, b):
    for turple in b:
        a.append(turple)
    return a


def normalize(net):
    netList = net[1]
    length = int(len(netList)/2)
    sum = 0
    for i in range(0, length):
        sum += netList[2*i+1]
    for i in range(0, length):
        netList[2*i+1] = netList[2*i+1] / sum
    return (net[0], netList)


def turnPairs(file):
    '''dict_ = open("people_name_list.txt")
    dict_words = dict_.readlines()
    for word in dict_words:
        word = ''.join(re.findall(r'[\u4e00-\u9fa5]', word))
        jieba.add_word(word, tag="my_name")
    dict_.close()'''
    path = file[0].split("/")
    key = int(path[len(path)-1][2:4])
    lines = file[1].split("\n")
    values = []
    for line in lines:
        '''line = ''.join(re.findall(r'[\u4e00-\u9fa5]', line))
        words = pseg.lcut(line)
        nameStr = ''
        for word in words:
            if word.flag == "my_name":
                nameStr = nameStr + ' ' + word.word
        nameStr = nameStr.strip()
        values.append((key, nameStr))'''
        values.append((key, line))
    return values

'''sc = SparkContext(appName='test')
file = sc.textFile("novels")
#file = sc.wholeTextFiles("novels").flatMap(lambda x: turnPairs(x)).map(lambda x: split_line(x)).sortBy(lambda x: x[1], True).partitionBy(14, lambda x: x-1)
splitNameFile = file.map(lambda line: split_line(line))
splitNameFile.filter(lambda words: len(words) > 0).sortBy(lambda x: x, True, 1).saveAsTextFile("sss")'''


sc = SparkContext(appName='test')
file = sc.textFile("test-out")
distinctNameFile = file.map(lambda line: line.strip("\t")).map(lambda line: line.split(" ")).map(lambda words: distinct_name(words)).filter(lambda words: len(words) > 0)
turples = distinctNameFile.flatMap(lambda names: generateRelation(names)).filter(lambda turples: len(turples) > 0).reduceByKey(lambda a, b: a+b)
network = turples.map(lambda relation: generateNet(relation)).reduceByKey(lambda a, b: mergeList(a, b)).map(lambda net: normalize(net))
network.sortBy(lambda x: x, True, 1).saveAsTextFile("result1")

#PageRank
time = 10
lines = network
links = lines.map(lambda line: (line[0], line[1]))
ranks = links.mapValues(lambda x: 1.0)
for i in range(time):
    ranks = links.join(ranks).flatMap(split_links).reduceByKey(lambda x, y: x + y)
ranks.sortBy(lambda x: x[1], False, 1).saveAsTextFile("result2")

