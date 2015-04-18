import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 
 * class is used to monitor the threads that are being executed
 * prints to the console the number of tasks being carried out
 * number of threads active, how many tasks have been completed
 * and whether or not the threadpool has been terminated
 * 
 */
public class MonitorThread implements Runnable {
	
	// threadpool to be monitored
	private ExecutorService executor; 

	// delay between each monitor update
	private int delay; 
	private boolean run = true;

	public MonitorThread(ExecutorService executer, int delay) {
		this.executor = executer;
		this.delay = delay;
	}

	// This shuts down the monitor thread after pausing for 5 milliseconds
	public void shutdown() { 
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.run = false;
	}

	@Override
	public void run() {
		
		// prints out thread info until monitor thread shutdown
		while (run) { 

			System.out.println(String.format("[monitor] [%d/%d] Active: %d, Completed: %d, Tasks Running: %d, "
									+ "All Threads Complete: %s",
									((ThreadPoolExecutor) this.executor).getPoolSize(),
									((ThreadPoolExecutor) this.executor).getCorePoolSize(),
									((ThreadPoolExecutor) this.executor).getActiveCount(),
									((ThreadPoolExecutor) this.executor).getCompletedTaskCount(),
									((ThreadPoolExecutor) this.executor).getTaskCount(), 
									this.executor.isTerminated())); // true if all tasks were completed

			try {
				// length between each monitor update determined by dealy
				Thread.sleep(delay); 
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
