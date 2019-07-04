package PageRank;

public class PageRankDriver {
    private static int times = 10;

    public static void main(String[] args) throws Exception{

        //if (args == null || args.length != 2) {
           // throw new RuntimeException("请输入输入路径、输出路径");
        //}

        String[] forGB = {"../task2-output.txt", "../Data0"};
        //forGB[0] = args[0];
        GraphBuilder.graphBuilder(forGB);

        String[] forItr = {"../Data0", "../Data1"};
        for(int i = 0; i < times; i++){
            forItr[0] = "../Data" + (i);
            forItr[1] = "../Data" + (i + 1);
            PageRankIter.pageRankIter(forItr);
        }

        String[] forRV = {"../Data" + times, "../task3-output"};
        RankViewer.rankViewer(forRV);
    }
}
