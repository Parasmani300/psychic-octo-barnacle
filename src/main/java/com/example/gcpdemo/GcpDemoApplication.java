package com.example.gcpdemo;

import com.example.gcpdemo.service.GCSOperations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class GcpDemoApplication implements CommandLineRunner {

	@Autowired
	GCSOperations gcsOperations;

	public static void main(String[] args) {
		SpringApplication.run(GcpDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		gcsOperations.getFileNamesInBucket("paras-mani.appspot.com","check");
		gcsOperations.getFileNamesInBucketMod("paras-mani.appspot.com","check");
	}
}
