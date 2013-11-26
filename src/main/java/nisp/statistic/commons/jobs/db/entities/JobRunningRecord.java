/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nisp.statistic.commons.jobs.db.entities;

/**
 *
 * @author Administrator
 */
public class JobRunningRecord {

    private long id;
    private JobEntity jobEntiry;
    private long startTime;
    private long endTime;
    private int jobState;
    private String jobFailaureInfo;
    
    
    
}
