import csv

if __name__ == '__main__':
    file="task3-output.txt"
    with open(file,"r",encoding="utf-8") as f:
        text=f.readlines()
    edges=[]
    for line in text:
        target=line.split("\t")[1]
        target_nodes=target.split("|")
        for target_node in target_nodes:
            edge = []
            edge.append(line.split("\t")[0])
            edge.append(target_node.split(" ")[0])
            edge.append(target_node.split(" ")[1].replace('\n',""))
            edges.append(edge)

    ft = open("edge.csv", "w", newline='',encoding="utf-8")  # 创建新的csv数据文件（用项目名为其命名）
    writer=csv.writer(ft)
    writer.writerow(("Source","Target","Weight"))
    for term in edges:
        writer.writerow([term[0],term[1],term[2]])

    file2 = "task7-output.txt"
    with open(file2, "r", encoding="utf-8") as f2:
        text = f2.readlines()
    print(text)
    ft2 = open("node.csv", "w", newline='', encoding="utf-8")  # 创建新的csv数据文件（用项目名为其命名）
    writer = csv.writer(ft2)
    writer.writerow(("modularity_class","Id"))
    nodes=[]
    for line in text:
        line=line.replace(" \n","")
        modularity_class=line.split("\t")[0]
        Ids=line.split("\t")[1].split(" ");
        for id in Ids:
            node=[]
            node.append(modularity_class)
            node.append(id)
            nodes.append(node)
    for term in nodes:
        writer.writerow((term[0],term[1]))




