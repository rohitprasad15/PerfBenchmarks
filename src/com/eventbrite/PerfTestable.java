package com.eventbrite;

public interface PerfTestable {
	public boolean setup(String config);
	public long work(long units);
	public boolean teardown();
	public boolean setWorkItemFile(String fileName);
	public boolean useRandomString(int size);
}
