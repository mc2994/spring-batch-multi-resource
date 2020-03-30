package com.infotech.batch.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.infotech.batch.model.Person;

public interface UserRepository extends JpaRepository<Person, Integer> {
	
}