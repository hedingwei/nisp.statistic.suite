/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr.ta.userflow;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Administrator
 */
public class Main1 {

    public static void main(final String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse(args[1]);
        int days = Integer.parseInt(args[2]);
        for (int i = 0; i < days; i++) {
            final String d = sdf.format(new Date(date.getTime() + (i * 24 * 60 * 60 * 1000l)));
            new Thread(new Runnable() {
                public void run() {
                    try {
                        new Main12().main(new String[]{args[0], d});
                    } catch (IOException ex) {
                        Logger.getLogger(Main1.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }).start();

        }
    }
}
