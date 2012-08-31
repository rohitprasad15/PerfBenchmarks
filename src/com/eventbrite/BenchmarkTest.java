package com.eventbrite;

public class BenchmarkTest {
	public String configForTarget = "addressess=qa-fh-mq1:5672,qa-fh-mq2:5672,qa-fh-mq3:5672,qa-fh-mq4:5672;" +
									"exchange=eb.logs.web_analytics;pub-acks=false;queue=apistats";
	private long completionTime;
	public long numOfRounds = 10;
	public long unitsPerRound = 60000;
	public long[] timingsPerRound;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BenchmarkTest test = new BenchmarkTest();
		if (args.length > 1) {
			test.configForTarget = args[1];
		}
		PerfTestable testableTarget = new RabbitProducerTest();
		test.runPerfTest(testableTarget);
		test.displayResults();
		System.out.println("Total Time for completion: " + test.getCompletionTime());
	}

	private void displayResults() {
		long oneMillion = (1000 * 1000);
		System.out.println("Time taken to send " + unitsPerRound + " msgs in microsec. Ran for " + numOfRounds + " rounds.");
		for (long timeTaken : timingsPerRound) {
			double msgPerSecond = (unitsPerRound / (Double.parseDouble(Long.toString(timeTaken)) / (double)1000000)) * 1000;
			//msgPerSecond = unitsPerRound / ((double)timeTaken / oneMillion) * 1000; 
			System.out.println(Long.toString(timeTaken/oneMillion) + "," + Double.toString(msgPerSecond)  );
		}
	}

	private void runPerfTest(PerfTestable testableTarget) {
		if (!testableTarget.setup(configForTarget)) {
			System.out.println("Error in setting up target.\n");
			return;
		}
		testableTarget.useRandomString(1000);
		
		// Warm up. Allow TCP window to adjust and optimize itself.
		testableTarget.work(5000);

		long[] perRoundCompletionTime = new long[(int) numOfRounds];
		long startOfTest = System.nanoTime();
		for (int i = 0; i < numOfRounds; i++) {
			// Each round of test
			long startOfRound = System.nanoTime();
			if (testableTarget.work(unitsPerRound) != unitsPerRound) {
				break;
			}
			long endOfRound = System.nanoTime();
			perRoundCompletionTime[i] = endOfRound - startOfRound;
			System.out.println("Just did a lap." + perRoundCompletionTime[i]);
		}
		long endOfTest = System.nanoTime();
		setCompletionTime(endOfTest - startOfTest);
        timingsPerRound = perRoundCompletionTime;

		// Cool down. Allow other nodes to lag while stopping.
		testableTarget.work(5000);
		
		testableTarget.teardown();
	}

	public void setCompletionTime(long completionTime) {
		this.completionTime = completionTime;
	}

	public long getCompletionTime() {
		return completionTime;
	}

}
