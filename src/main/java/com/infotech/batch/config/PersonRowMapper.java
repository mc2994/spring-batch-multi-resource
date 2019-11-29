package com.infotech.batch.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import com.infotech.batch.model.Person;


public class PersonRowMapper implements RowMapper<Person> {

	@Override
	public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
		Person person = new Person();
		person.setId(rs.getLong("id"));
		person.setFirstName(rs.getString("first_name"));
		person.setLastName(rs.getString("last_name"));
		person.setEmail(rs.getString("email"));
		person.setAge(rs.getInt("age"));
		System.out.println("Person: "+person.toString());
		return person;
	}

}
