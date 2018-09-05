package logmerge;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import logmerge.cli.CliOptions;

public class LogMerger {
	private static final Logger LOGGER = Logger.getLogger(LogMerger.class.getName());
	private final CliOptions cliOptions;

	public LogMerger(CliOptions cliOptions) {
		this.cliOptions = cliOptions;
	}

	public void merge() {
		List<LogFile> logFiles = null;
		try (OutputStream outputStream = createOutputStream(cliOptions)) {
			logFiles = createLogFiles(cliOptions);
			LogFile nextLogFile = getNextLogFile(logFiles);
			while (nextLogFile != null) {
				String markerString = "[" + nextLogFile.getMarker() + "]" + cliOptions.getDelimiter();
				Iterator<String> iterator = nextLogFile.getNextLines();
				while (iterator.hasNext()) {
					String line = iterator.next();
					if (cliOptions.isMarker()) {
						outputStream.write(markerString.getBytes("UTF-8"));
					}
					outputStream.write(line.getBytes("UTF-8"));
					outputStream.write('\n');
				}
				outputStream.flush();
				nextLogFile = getNextLogFile(logFiles);
			}
		} catch (IOException e) {
			throw new LogMergeException(LogMergeException.Reason.IOException,
					"Merging files failed due to I/O error: " + e.getMessage(), e);
		} finally {
			if (logFiles != null) {
				for (LogFile logFile : logFiles) {
					try {
						logFile.close();
					} catch (Exception ignored) {
					}
				}
			}
		}
	}

	private OutputStream createOutputStream(CliOptions cliOptions) throws IOException {
		OutputStream returnValue;
		String outputFile = cliOptions.getOutputFile();
		if (outputFile == null) {
			returnValue = System.out;
		} else {
			returnValue = cliOptions.isGzippedOutput() ? gzippedOutputStream(outputFile) : textOutputStream(outputFile);
		}
		return returnValue;
	}

	private GZIPOutputStream gzippedOutputStream(String outputFile) throws FileNotFoundException, IOException {
		return new GZIPOutputStream(new FileOutputStream(new File(outputFile)));
	}

	private FileOutputStream textOutputStream(String outputFile) throws FileNotFoundException {
		return new FileOutputStream(new File(outputFile));
	}

	private LogFile getNextLogFile(List<LogFile> logFiles) throws IOException {
		Date currentDate = null;
		LogFile currentLogFile = null;
		for (LogFile logFile : logFiles) {
			Date date = logFile.peekNextTimestamp();
			if (date != null) {
				if (currentDate == null) {
					currentDate = date;
					currentLogFile = logFile;
				} else {
					if (date.before(currentDate)) {
						currentDate = date;
						currentLogFile = logFile;
					}
				}
			}
		}
		return currentLogFile;
	}

	private List<LogFile> createLogFiles(CliOptions cliOptions) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(cliOptions.getTimestampFormat());
		List<LogFile> logFiles = new ArrayList<>(cliOptions.getLogFiles().size());
		int marker = 0;
		for (String fileName : cliOptions.getLogFiles()) {
			try {
				BufferedReader fileReader = createBufferedReader(fileName);
				LogFile logFile = new LogFile(fileName, fileReader, simpleDateFormat, cliOptions, marker++);
				logFiles.add(logFile);
			} catch (FileNotFoundException e) {
				throw new LogMergeException(LogMergeException.Reason.FileNotFound,
						"File not found: " + e.getMessage());
			} catch (UnsupportedEncodingException e) {
				throw new LogMergeException(LogMergeException.Reason.UnsupportedEncoding,
						"Unsupported encoding: " + e.getMessage());
			} catch (IOException e) {
				throw new LogMergeException(LogMergeException.Reason.IOException,
						"Generic IO exception: " + e.getMessage());
			}
		}
		return logFiles;
	}

	private BufferedReader createBufferedReader(final String fileName) throws IOException {
		final boolean isGzipped = checkIfFileIsGzipped(fileName);
		InputStream fileInputStream = new FileInputStream(fileName);
		if (isGzipped) {
			return new BufferedReader(
					new InputStreamReader(new GZIPInputStream(fileInputStream), StandardCharsets.UTF_8));
		} else {
			return new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.UTF_8));
		}
	}

	private boolean checkIfFileIsGzipped(String fileName) throws IOException {
		try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName))) {
			byte[] magicHeader = new byte[2];
			bufferedInputStream.read(magicHeader);
			return magicHeader[0] == (byte) 0x1f && magicHeader[1] == (byte) 0x8b;
		}
	}
}
