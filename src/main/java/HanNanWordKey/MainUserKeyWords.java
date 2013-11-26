package HanNanWordKey;

import mr.uba.searchkyes.Main;
import java.io.IOException;
import java.util.List;

public class MainUserKeyWords {

    public static void main(String[] args) throws IOException {
        GetFileList gList = new GetFileList("");
        Main userMain = new Main();
        List<String> fileList = gList.getList("/mapreduce/UserBehaviorAnalysis/uba/src", args[1], args[2]);
        String[] fl = new String[fileList.size()];
        for (int i = 0; i < fileList.size(); i++) {
            fl[i] = fileList.get(i);
        }
//        userMain.executeMapReduce(fl, args[0]);
    }
}
