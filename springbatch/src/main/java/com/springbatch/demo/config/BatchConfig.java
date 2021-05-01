package com.springbatch.demo.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.springbatch.demo.model.Product;

@Configuration
public class BatchConfig {

	@Autowired
	private StepBuilderFactory sbf;
	
	@Autowired
	private JobBuilderFactory jbf;
	
	@Bean
	public Step step() {
		return sbf.get("s1")
				.<Product,Product>chunk(1)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
	}
	
	@Bean
	public Job job() {
		return jbf.get("j1")
				.incrementer(new RunIdIncrementer())
				.start(step())
				.build();
	}
	@Bean
	public ItemReader<Product> reader() {
		FlatFileItemReader<Product> reader = new FlatFileItemReader<Product>();
		reader.setResource(new ClassPathResource("products.csv"));

		DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<Product>();

		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames("id", "name", "desc", "price");

		BeanWrapperFieldSetMapper<Product> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<Product>();
		lineMapper.setLineTokenizer(lineTokenizer);

		lineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);
		reader.setLineMapper(lineMapper);
		return reader;
	}

	@Bean
	public ItemProcessor<Product, Product> processor() {
		return (p) -> {
			p.setPrice(0.9 * p.getPrice());
			return p;
		};
	}

	@Bean
	public ItemWriter<Product> writer() {
		
		JdbcBatchItemWriter<Product> jdbcBatchItemWriter = new JdbcBatchItemWriter<Product>();
		jdbcBatchItemWriter.setDataSource(dataSource());
		jdbcBatchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Product>());
		jdbcBatchItemWriter.setSql("INSERT INTO PRODUCT (ITEM, NAME, DESC, PRICE) VALUES(:id,:name,:desc,:price)");
		return jdbcBatchItemWriter;
	}
	
	
	  @Bean public DataSource dataSource() { DriverManagerDataSource dataSource=new
	  DriverManagerDataSource(); dataSource.setDriverClassName("org.h2.Driver");
	  dataSource.setUrl("jdbc:h2:tcp://localhost/~/mydb");
	  dataSource.setUsername("sa"); dataSource.setPassword(""); return dataSource;
	  
	  }
	 
}
