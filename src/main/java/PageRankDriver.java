package PageRank;

public class PageRankDriver {
    private static int times = 2;

    public static void main(String[] args) throws Exception{

        //if (args == null || args.length != 2) {
           // throw new RuntimeException("请输入输入路径、输出路径");
        //}

        String[] forGB = {"/home/jingtao/Java/Final/task2-output.txt", "/home/jingtao/Java/Final/Data0"};
        //forGB[0] = args[0];
        GraphBuilder.graphBuilder(forGB);

        String[] forItr = {"/home/jingtao/Java/Final/Data0", "/home/jingtao/Java/Final/Data1"};
        for(int i = 0; i < times; i++){
            forItr[0] = "/home/jingtao/Java/Final/Data" + (i);
            forItr[1] = "/home/jingtao/Java/Final/Data" + (i + 1);
            PageRankIter.pageRankIter(forItr);
        }

        String[] forRV = {"/home/jingtao/Java/Final/Data" + times, "/home/jingtao/Java/Final/task3-output.txt"};
        RankViewer.rankViewer(forRV);
    }
}
