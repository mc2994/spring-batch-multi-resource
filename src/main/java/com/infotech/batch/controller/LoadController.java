package com.infotech.batch.controller;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/load")
public class LoadController {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;

	@GetMapping("/batchStart")
	public BatchStatus load() throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException {

		Map<String, JobParameter> maps = new HashMap<>();
		maps.put("time", new JobParameter(System.currentTimeMillis()));
		JobParameters parameters = new JobParameters(maps);
		JobExecution jobExecution = jobLauncher.run(job, parameters);

		System.out.println("JobExecution: " + jobExecution.getStatus());

		System.out.println("Batch is Running...");
		while (jobExecution.isRunning()) {
			System.out.println("...");
		}

		return jobExecution.getStatus();
	}

	@PostMapping("/processUploadFile")
	public String processUploadFile(@RequestParam("file") List<MultipartFile> multipartFile)
			throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, IOException {
		
		//read uploaded file and put to input folder
		File fileToImport = null;
		for (MultipartFile file : multipartFile) {
			String path = new File("src/main/resources/input").getAbsolutePath() + "\\";
			fileToImport = new File(path + file.getOriginalFilename());
			file.transferTo(fileToImport);
		}

		// Launch the Batch Job
		Map<String, JobParameter> maps = new HashMap<>();
		maps.put("time", new JobParameter(System.currentTimeMillis()));
		JobParameters parameters = new JobParameters(maps);

		// System.out.println(">>>>>>>>>>>>>>>> "+fileToImport.getAbsolutePath());
//        JobExecution jobExecution = jobLauncher.run(job, new JobParametersBuilder()
//                .addString("fullPathFileName", fileToImport.getAbsolutePath())
//                .addDate("time", new Date())
//                .toJobParameters());

		JobExecution jobExecution = jobLauncher.run(job, parameters);
		System.out.println("JobExecution: " + jobExecution.getStatus());
		return "sdadad";
	}
}