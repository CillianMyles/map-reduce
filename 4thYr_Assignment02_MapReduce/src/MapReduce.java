import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {

	public static void main(String[] args) {

		Map<String, String> input = new HashMap<String, String>();

		// user inputs number of files to be inputted
		System.out.println("Please enter the number of files you wish to Index");
		Scanner numFilesScanner = null;
		numFilesScanner = new Scanner(System.in);
		int numFiles = 0;
		numFiles = numFilesScanner.nextInt(); 

		// string array to store name and file contents in string format
		String[] FileArray = new String[numFiles * 2]; 

		String filePath = null;
		Scanner filePathScanner = null;
		BufferedReader reader = null;
		
		// String Buffer to store file contents
		StringBuffer fileData = new StringBuffer(); 
		
		int j = 0;
		int k = 1;
		
		// for loop takes in file contents
		for (int i = 0; i < numFiles; i++) { 
			
			boolean correctPath = false;
			while (!correctPath) {
				try {
					// user inputs files paths
					System.out.println("Please specify a path for file " + (i + 1));
					filePathScanner = new Scanner(System.in);
					filePath = filePathScanner.nextLine();
					reader = new BufferedReader(new FileReader(filePath));
					correctPath = true;
					
				} catch (FileNotFoundException e) {
					System.out.println("*** file path was entered incorrectly ***");
				}
			}
			correctPath = false;

			char[] buf = new char[1024];
			int numRead = 0;
			try {
				// Data is read from file and appended to fileData
				while ((numRead = reader.read(buf)) != -1) { 
					String readData = String.valueOf(buf, 0, numRead);
					fileData.append(readData);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			// contents of the file stored in fileData, converted to a string
			String output = fileData.toString(); 
			// filename added to FileArray in the odd positions
			FileArray[j] = filePath; 
			// file contents added to FileArray in the even positions
			FileArray[k] = output; 

			j = j + 2;
			k = k + 2;
		}
		
		// user defines number of threads to be used 
		System.out.println("Please enter the number of threads you would like to use");
		Scanner numThreadsScanner = null;
		numThreadsScanner = new Scanner(System.in);
		int numThreads = 0;
		numThreads = numThreadsScanner.nextInt();
		
		// creating a pool of threads
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
		
        // adding a monitoring thread 
        MonitorThread monitor = new MonitorThread(pool, 8);
        Thread monitorThread = new Thread(monitor);
        monitorThread.start();
		
		// close the scanner inputs
		numFilesScanner.close();
		numThreadsScanner.close();
		filePathScanner.close();
		
		int p = 0;
		int c = 1;

		// Pass the filenames and content to the Input function.
		for (int x = 0; x < numFiles; x++) {
			input.put(FileArray[p], FileArray[c]);
			p = p + 2;
			c = c + 2;
		}

		final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

		// ---------- MAP ---------- 

		final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

		final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
			@Override
			public synchronized void mapDone(String file, List<MappedItem> results) {
				mappedItems.addAll(results);
			}
		};

		List<Thread> mapCluster = new ArrayList<Thread>(input.size());

		Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
		while (inputIter.hasNext()) {
			Map.Entry<String, String> entry = inputIter.next();
			final String file = entry.getKey();
			final String contents = entry.getValue();

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					map(file, contents, mapCallback);
				}
			});
			
			mapCluster.add(t);
			pool.execute(t); // multithreaded execution
		}
		
		pool.shutdown();

		// wait for mapping phase to be over
		for (Thread t : mapCluster) {
			try {
				t.join();
        	} catch(InterruptedException e) {						
        		throw new RuntimeException(e);
        	}
		}
		
		// terminate the monitoring thread
		monitor.shutdown();

		// ---------- GROUP ---------- 

		Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

		Iterator<MappedItem> mappedIter = mappedItems.iterator();
		while (mappedIter.hasNext()) {
			MappedItem item = mappedIter.next();
			String word = item.getWord();
			String file = item.getFile();
			char firstChar = word.charAt(0);
						
			// if first letter of word is an actual letter (e.g. not a character)
			if (Character.isLetter(firstChar)) {
				
				String firstLetter = word.substring(0, 1).toLowerCase();
				
				// add the first letters to a list 
				List<String> list = groupedItems.get(firstLetter);
				if (list == null) {
					list = new LinkedList<String>();
					groupedItems.put(firstLetter, list);
				}
				list.add(file);
			}		
		}

		// ---------- REDUCE ---------- 

		final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
			@Override
			public synchronized void reduceDone(String key, Map<String, Integer> value) {
				output.put(key, value);
			}
		};

		List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

		Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
		while (groupedIter.hasNext()) {
			Map.Entry<String, List<String>> entry = groupedIter.next();
			final String firstLetter = entry.getKey();
			final List<String> list = entry.getValue();

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					reduce(firstLetter, list, reduceCallback);
				}
			});
			reduceCluster.add(t);
			t.start(); // not multithreaded
		}

		// wait for reducing phase to be over
		for (Thread t : reduceCluster) {
			try {
				t.join();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		System.out.println("\nOUTPUT\n" + output);
	}

	public static void map(String file, String contents, List<MappedItem> mappedItems) {
		String[] letters = contents.trim().split("\\s+");
		for (String firstLetter : letters) {
			mappedItems.add(new MappedItem(firstLetter, file));
		}
	}

	public static void reduce(String firstLetter, List<String> list, Map<String, Map<String, Integer>> output) {
		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		output.put(firstLetter, reducedList);
	}

	public static interface MapCallback<E, V> {

		public void mapDone(E key, List<V> values);
	}

	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		
		String[] letters = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(letters.length);
		
		for (String firstLetter : letters) {
			results.add(new MappedItem(firstLetter, file));
		}
		callback.mapDone(file, results);
	}

	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K, V> results);
	}

	public static void reduce(String firstLetter, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(firstLetter, reducedList);
	}

	// Function to read the file
	public static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}
}