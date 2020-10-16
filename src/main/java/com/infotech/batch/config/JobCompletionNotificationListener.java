package com.infotech.batch.config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("!!! JOB FINISHED! Time to verify the results");
			File folder = new File("src/main/resources/input/");
			File[] files = folder.listFiles();

			for (File file : files) {
				String newFileName = createNewFileName(file.getName());
				String dst = "output/" + newFileName;

				if (file.isFile()) {
					try {
						moveFile(file.getAbsolutePath(), dst);
					} catch (Exception e) {
						throw new UnexpectedJobExecutionException("Could not delete file ");
					}
				}
			}
		}
	}

	private boolean moveFile(String sourcePath, String targetPath) {
		boolean fileMoved = true;
		try {
			Files.move(Paths.get(sourcePath), Paths.get(targetPath), StandardCopyOption.REPLACE_EXISTING);
		} catch (Exception e) {
			fileMoved = false;
			e.printStackTrace();
		}
		return fileMoved;
	}

	private String createNewFileName(String file) {
		String fileName = "";
		try {
			LocalDateTime date = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_hh_mm_ss");
			fileName = date.format(formatter) + "_" + file;
		} catch (DateTimeException exception) {
			exception.printStackTrace();
		}
		return fileName;
	}
}