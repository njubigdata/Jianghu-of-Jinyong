import csv
from py2neo import Graph, Node, Relationship, NodeMatcher

graph = Graph(
        "http://localhost:7474",
        username="JiangHu",
        password="12345678"
    )

buffer={}

def gen_node_csv():
    file2 = "task7-output.txt"
    with open(file2, "r", encoding="utf-8") as f2:
        text2 = f2.readlines()
    # print(text)

    file3 = "task4-output.txt"
    with open(file3, "r", encoding="utf-8") as f3:
        text3 = f3.readlines()

    dict = {}
    for line in text3:
        line = line.replace("\n", "")
        name = line.split('\t')[1]
        PR = line.split('\t')[0]
        dict[name] = PR

    print(len(dict))
    print(dict)

    ft2 = open("node.csv", "w", newline='', encoding="utf-8")
    writer = csv.writer(ft2)
    writer.writerow(("Book", "Id", "PageRank"))
    nodes = []
    for line in text2:
        line = line.replace(" \n", "")
        modularity_class = line.split("\t")[0]
        Ids = line.split("\t")[1].split(" ")
        for id in Ids:
            PR = dict[id]
            node = []
            node.append(modularity_class)
            node.append(id)
            node.append(PR)
            nodes.append(node)
    for term in nodes:
        writer.writerow((term[0], term[1], term[2]))

def gen_edge_csv():
    file = "task3-output.txt"
    with open(file, "r", encoding="utf-8") as f:
        text = f.readlines()
    edges = []
    for line in text:
        target = line.split("\t")[1]
        target_nodes = target.split("|")
        for target_node in target_nodes:
            edge = []
            edge.append(line.split("\t")[0])
            edge.append(target_node.split(" ")[0])
            edge.append(target_node.split(" ")[1].replace('\n', ""))
            edges.append(edge)

    ft = open("edge.csv", "w", newline='', encoding="utf-8")
    writer = csv.writer(ft)
    writer.writerow(("Source", "Target", "Weight"))
    for term in edges:
        writer.writerow([term[0], term[1], term[2]])


book_list = ['白马啸西风','碧血剑','飞狐外传','连城诀','鹿鼎记','射雕英雄传'
            ,'神雕侠侣','书剑恩仇录','天龙八部','侠客行','笑傲江湖','雪山飞狐'
            ,'倚天屠龙记','鸳鸯刀','越女剑']

def search_name(str,offset,length):
    if str in buffer:
        length.append(len(buffer[str]))
        return buffer[str][min(offset,len(buffer[str])):min(offset+10,len(buffer[str]))]

    if str in book_list:
        instr = """
                match (n) where n.Book = '""" + str + """'
                return n.Name as Name, n.PageRank as PageRank, n.First as First
            """
        #print(instr)
        data = graph.run(instr)

        tuple = []
        for item in data:
            tuple.append((item['Name'], item['PageRank'], item['First']))

        tuple = sorted(tuple, key=lambda w: float(w[1]), reverse=True)
        if len(length) == 0:
            length.append(len(tuple))
        buffer[str] = tuple
        # for i in tuple:
        #      print(i)
        return tuple[min(offset,len(buffer[str])):min(offset+10,len(buffer[str]))]
    else:
        if str in buffer:
            length.append(len(buffer[str]))
            return buffer[str][min(offset, len(buffer[str])):min(offset + 10, len(buffer[str]))]
        instr = """
            match (n) where n.Name = '""" + str + """'
            return n.Book as Book, n.First as First
        """
        #print(instr)
        data = graph.run(instr)
        for item in data:
            accurate_book=item['Book']
            accurate_first=item['First']

        instr = """
            match (n)-[r]->(m) where n.Name = '""" + str + """'
            return r.Weight as Weight, m.Name as Name, m.PageRank as PageRank, m.First as First
        """
        #print(instr)
        data = graph.run(instr)

        tuple = []
        for item in data:
            tuple.append( ( item['Name'], item['Weight'], item['First']) )

        tuple = sorted(tuple, key=lambda w: float(w[1]), reverse=True)
        if len(tuple)!=0:
            tuple.insert(0,(accurate_book,accurate_first))
        # for i in tuple:
        #     print(i)
        if len(length) == 0:
            length.append(len(tuple))
        buffer[str] = tuple
        # for i in tuple:
        #      print(i)
        return tuple[min(offset,len(buffer[str])):min(offset+10,len(buffer[str]))]


if __name__ == '__main__':

    # gen_node_csv()
    # gen_edge_csv()

    #以下用于将csv文件内容导入数据库
    # LP_correct = 0
    #
    # file = "node.csv"
    # f = open(file, "r", encoding='utf-8')
    # csv_reader_lines = csv.reader(f)
    # next(csv_reader_lines)
    # count = 1
    # for line in csv_reader_lines:
    #     book = line[0]
    #     id = line[1]
    #     pr = line[2]
    #     first_appear = ""
    #     novel_file = "novels/"+book+".txt"
    #     fp = open(novel_file, "r", encoding='utf-8')
    #     text = fp.readlines()
    #     for paragraph in text:
    #         if id in paragraph:
    #             first_appear = paragraph
    #             LP_correct = LP_correct+1
    #             break
    #     print(str(count)+": "+book+" "+id)
    #     new_node = Node("Character", Book = book, Name = id, PageRank = pr, First = first_appear)
    #     graph.create(new_node)
    #     count = count+1
    #
    # print(LP_correct)
    # print(str(LP_correct / count))
    #
    # instr = """
    # CREATE INDEX ON:Character(Name)
    # """
    # graph.run(instr)
    #
    # file = "edge.csv"
    # f = open(file, "r", encoding='utf-8')
    # csv_reader_lines = csv.reader(f)
    # next(csv_reader_lines)
    # count = 1
    # for line in csv_reader_lines:
    #     src = line[0]
    #     dst = line[1]
    #     weight = line[2]
    #     instr = """
    #         match (a:Character),(b:Character)
    #         where a.Name = '""" + src + """' and b.Name = '""" + dst + """'
    #         create (a)-[r:has_link{Weight:'"""+ weight +"""'}]->(b)
    #     """
    #     graph.run(instr)
    #     print(str(count)+": "+src+"--"+dst)
    #     count = count +1
    #
    # print(LP_correct)
    # print(LP_correct/1248)

    length=[]
    search_name("神雕侠侣",0,length)
    search_name("郭靖",0,length)




