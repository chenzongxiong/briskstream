package applications.spout;

import applications.constants.BaseConstants;
import applications.spout.helper.wrapper.StringStatesWrapper;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Scanner;

public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    protected ArrayList<char[]> array;
    protected int element = 0;
    protected int counter = 0;
    //	String[] array_array;
    char[][] array_array;
    private transient BufferedWriter writer;
    private int cnt;
    private int taskId;


    public MemFileSpout() {
        super(LOG);
        this.scalable = false;
    }

    @Override
    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("Spout initialize is being called");
        long start = System.nanoTime();
        cnt = 0;
        counter = 0;
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..

        // numTasks = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_THREADS));

        String OS_prefix = null;

        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }
        String path;

        if (OsUtils.isMac()) {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_TEST_PATH)));
        } else {
            path = config.getString(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH)));
        }

        String s = System.getProperty("user.home").concat("/data/app/").concat(path);

        array = new ArrayList<>();
        try {
            openFile(s);
        } catch (FileNotFoundException e) {

            // s = "/data/DATA/tony/data/".concat(path);
            // s = "/home/zongxiong/briskstream/dataset/wordcount/Skew0.dat";
            s = "/home/zxchen/briskstream/dataset/wordcount/Skew0.dat";
            try {
                openFile(s);
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            }
        }
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("JVM PID  = " + pid);

        int end_index = array_array.length * config.getInt("count_number", 1);

        LOG.info("spout:" + this.taskId + " elements:" + end_index);
        long end = System.nanoTime();
        LOG.info("spout prepare takes (ms):" + (end - start) / 1E6);
    }

    /**
     * relax_reset source messages.
     */
    @Override
    public void cleanup() {

    }

    private void build(Scanner scanner) {
        cnt = 100;
        if (config.getInt("batch") == -1) {
            while (scanner.hasNext()) {
                array.add(scanner.next().toCharArray());//for micro-benchmark only
            }
        } else {

            if (!config.getBoolean("microbenchmark")) {//normal case..
                //&& cnt-- > 0
                if (OsUtils.isMac()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        array.add(scanner.nextLine().toCharArray());
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        array.add(scanner.nextLine().toCharArray()); //normal..
                    }
                }

            } else {
                int tuple_size = config.getInt("size_tuple");
                LOG.info("Additional tuple size to emit:" + tuple_size);
                StringStatesWrapper wrapper = new StringStatesWrapper(tuple_size);
//                        (StateWrapper<List<StreamValues>>) ClassLoaderUtils.newInstance(parserClass, "wrapper", LOG, tuple_size);
                if (OsUtils.isWindows()) {
                    while (scanner.hasNextLine() && cnt-- > 0) { //dummy test purpose..
                        construction(scanner, wrapper);
                    }
                } else {
                    while (scanner.hasNextLine()) {
                        construction(scanner, wrapper);
                    }
                }
            }
        }
        scanner.close();
    }

    private void construction(Scanner scanner, StringStatesWrapper wrapper) {

        String splitregex = ",";
        String[] words = scanner.nextLine().split(splitregex);

        StringBuilder sb = new StringBuilder();

        for (String word : words) {
            sb.append(word).append(wrapper.getTuple_states()).append(splitregex);
        }

        array.add(sb.toString().toCharArray());


    }

    private void read(String prefix, int i, String postfix) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File((prefix + i) + "." + postfix), "UTF-8");
        build(scanner);
    }

    private void splitRead(String fileName) throws FileNotFoundException {
        int numSpout = this.getContext().getComponent(taskId).getNumTasks();
        int range = 10 / numSpout;//original file is split into 10 sub-files.
        int offset = this.taskId * range + 1;
        String[] split = fileName.split("\\.");
        for (int i = offset; i < offset + range; i++) {
            read(split[0], i, split[1]);
        }

        if (this.taskId == numSpout - 1) {//if this is the last executor of spout
            for (int i = offset + range; i <= 10; i++) {
                read(split[0], i, split[1]);
            }
        }
    }

    private void openFile(String fileName) throws FileNotFoundException {
        boolean split;

        split = !OsUtils.isMac() && config.getBoolean("split", true);

        if (split) {
            splitRead(fileName);
        } else {
            Scanner scanner = new Scanner(new File(fileName), "UTF-8");
            build(scanner);
        }

        array_array = array.toArray(new char[array.size()][]);
        counter = 0;

//		int bound = 0;
////		if (OsUtils.isMac()) {
////			bound = 805872;
////		} else {
////			bound = str_l.size();
////		}
//
//		bound = 100;
//
//		array = new char[bound][];//str_l.toArray(new String[str_l.size()]);
//
//

//		for (int i = 0; i < bound; i++) {
//			array[i] = str_l.get(i).toCharArray();
//		}
//

    }

    private void spout_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output")
                    + OsUtils.OS_wrapper("spout_threadId.txt")));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.relax_reset();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

//	protected void reset_index() {
//		if (timestamp_counter == array.length) {
//			timestamp_counter = 0;
//		}
//	}


//	int control = 1;

//	volatile String emit;


    @Override
    public void nextTuple() throws InterruptedException {
//        String[] value_list = new String[batch];
//        for (int i = 0; i < batch; i++) {
//            value_list[i] = array[timestamp_counter];
//        }
//
//		emit = array[timestamp_counter]+"";
//		if (control > 0) {
        collector.emit(array_array[counter]);//Arrays.copyOf(array_array[timestamp_counter], array_array[timestamp_counter].length) a workaround to ensure char array instead of string is used in transmission.
//		collector.emit_nowait(new StreamValues(array[timestamp_counter]));
        counter++;
        if (counter == array_array.length) {
            counter = 0;
        }
//		reset_index();
//			control--;
    }

    @Override
    public void nextTuple_nonblocking() throws InterruptedException {

//		collector.emit(array[timestamp_counter]);//Arrays.copyOf(array[timestamp_counter], array[timestamp_counter].length) a workaround to ensure char array instead of string is used in transmission.
        collector.emit_nowait(array_array[counter]);
        counter++;
        if (counter == array_array.length) {
            counter = 0;
        }

    }

    public void display() {
        LOG.info("timestamp_counter:" + counter);
    }

}
