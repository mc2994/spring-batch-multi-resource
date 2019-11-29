package com.infotech.batch.config;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.infotech.batch.model.Person;
import com.infotech.batch.repository.UserRepository;

@Component
public class DBWriter implements ItemWriter<Person> {

    @Autowired
    private UserRepository userRepository;

    @Override
    public void write(List<? extends Person> users) throws Exception {

        System.out.println("Data Saved for Users: " + users);
        userRepository.save(users);
    }
}