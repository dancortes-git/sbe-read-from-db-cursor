package br.com.dcc.springbatchexamples.configuration;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import br.com.dcc.springbatchexamples.domain.Customer;
import br.com.dcc.springbatchexamples.domain.mapper.CustomerRowMapper;
import br.com.dcc.springbatchexamples.listener.SimpleChunkListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class ReadFromDbCursorConfiguration {

	@Bean
	public JdbcCursorItemReader<Customer> readFromDatabaseCursorReader(DataSource dataSource) {
		JdbcCursorItemReader<Customer> reader = new JdbcCursorItemReader<>();
		reader.setSql("select id, email, firstName, lastName from customer order by id");
		reader.setDataSource(dataSource);
		reader.setRowMapper(new CustomerRowMapper());
		return reader;
	}

	@Bean
	public ItemWriter<Customer> readFromDatabaseCursorWriter() {
		return items -> {
			for (Customer item : items) {
				log.info("Writing item {}", item.toString());
			}
		};
	}

	@Bean
	public Step readFromDatabaseCursorStep1(StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
		return stepBuilderFactory.get("ReadFromDatabaseCursorStep1")
				.<Customer, Customer>chunk(50)
				.listener(new SimpleChunkListener())
				.reader(readFromDatabaseCursorReader(dataSource))
				.writer(readFromDatabaseCursorWriter())
				.build();
	}

	@Bean
	public Job readFromDatabaseCursorJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
		return jobBuilderFactory.get("ReadFromDatabaseCursorJob")
				.start(readFromDatabaseCursorStep1(stepBuilderFactory, dataSource))
				.build();

	}

}
