package com.infotech.batch.config;

import java.io.IOException;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.infotech.batch.model.Person;
import com.infotech.batch.processor.PersonItemProcessor;

@Configuration
@EnableScheduling
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Value(value = "classpath*:input/persons_*.csv")
	private Resource[] resources;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private JobLauncher jobLauncher;

	@Bean
	public JdbcCursorItemReader<Person> dbReader() {
		JdbcCursorItemReader<Person> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql("SELECT id,first_name,last_name,email,age FROM person");
		cursorItemReader.setRowMapper(new PersonRowMapper());
		cursorItemReader.setVerifyCursorPosition(false);
		return cursorItemReader;
	}

	@Bean
	public FlatFileItemReader<Person> reader() {
		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
		reader.setLineMapper(new DefaultLineMapper<Person>() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "firstName", "lastName", "email", "age" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
					{
						setTargetType(Person.class);
					}
				});
			}
		});
		return reader;
	}

	// orig with property resources
	@Bean
	public MultiResourceItemReader<Person> multiResourceItemReader1() {
		MultiResourceItemReader<Person> multiResourceItemReader = new MultiResourceItemReader<Person>();
		multiResourceItemReader.setResources(resources);
		multiResourceItemReader.setDelegate(reader());
		return multiResourceItemReader;
	}

	@Bean
	public MultiResourceItemReader<Person> multiResourceItemReader() {
		Resource[] resources = null;
		ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
		try {
			resources = patternResolver.getResources("classpath*:input/persons_*.csv");
		} catch (IOException e) {
			e.printStackTrace();
		}
		MultiResourceItemReader<Person> reader = new MultiResourceItemReader<>();
		reader.setResources(resources);
		reader.setDelegate(reader());
		return reader;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public DBWriter itemWriter() {
		return new DBWriter();
	}

	@Bean
	public FlatFileItemWriter<Person> writer() {
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<>();
		writer.setResource(new ClassPathResource("output/persons_output.csv"));
		writer.setName("testWriterName");
		writer.setLineAggregator(new DelimitedLineAggregator<Person>() {
			{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {
					{
						setNames(new String[] { "firstName", "lastName", "email", "age" });
					}
				});
			}
		});
		return writer;
	}

	@Bean
	public Job myJob() {
		return jobBuilderFactory.get("myJob").incrementer(new RunIdIncrementer()).start(readFromCSVToDB())
				.next(readFromDBToCSV()).build();
	}

	@Bean
	public Step readFromCSVToDB() {
		return stepBuilderFactory.get("readFromCSVToDB").<Person, Person>chunk(10).reader(multiResourceItemReader()) // read
																														// multiple
																														// csv
				.processor(processor()).writer(itemWriter()) // save data to db
				// .taskExecutor(taskAsync())
				.build();
	}

	@Bean
	public Step readFromDBToCSV() {
		return stepBuilderFactory.get("readFromDBToCSV").<Person, Person>chunk(10).reader(dbReader()) // read data from
																										// db
				.processor(processor()).writer(writer()) // write to csv file
				// .taskExecutor(taskAsync())
				.build();
	}

	@Bean
	public TaskExecutor taskAsync() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(5);
		System.out.println(">>>>>>>> ");
		return taskExecutor;
	}

	@Bean
	public ThreadPoolTaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(10);
		taskExecutor.setCorePoolSize(10);
		taskExecutor.setQueueCapacity(10);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	@Bean
	public ResourcelessTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	@Bean
	public MapJobRepositoryFactoryBean mapJobRepositoryFactory(ResourcelessTransactionManager txManager)
			throws Exception {
		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean(txManager);
		factory.afterPropertiesSet();
		return factory;
	}

	@Bean
	public JobRepository jobRepository(MapJobRepositoryFactoryBean factory) throws Exception {
		return (JobRepository) factory.getObject();
	}
	
	@Bean
	public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		return launcher;
	}

	// @Scheduled(cron = "0 */1 * * * ?")
	@Scheduled(fixedRate = 10000)
	public void perform() throws Exception {
//		JobParameters params = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
//				.toJobParameters();
//
//		System.out.println(">>>>>>>>>>>>>> " + System.currentTimeMillis());
//		jobLauncher.run(myJob(), params);
		
		
		System.out.println(" Job Started at :"+ new Date());
		JobParameters param = new JobParametersBuilder().addString("JobID",
		String.valueOf(System.currentTimeMillis())).toJobParameters();
		JobExecution execution = jobLauncher.run(myJob(), param);
		System.out.println("Job finished with status :" + execution.getStatus());
	}
}