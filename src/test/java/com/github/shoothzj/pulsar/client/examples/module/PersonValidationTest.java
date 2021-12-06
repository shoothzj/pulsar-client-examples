package com.github.shoothzj.pulsar.client.examples.module;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class PersonValidationTest {

    @Test
    public void testInvalidPersonAge() {
        Person person = new Person();
        person.setName("ZhangJian");
        person.setAge(17);
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Person>> violations = validator.validate(person);
        Assertions.assertEquals(1, violations.size());
    }

    @Test
    public void testInvalidPersonName() {
        Person person = new Person();
        person.setName(null);
        person.setAge(20);
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Person>> violations = validator.validate(person);
        Assertions.assertEquals(1, violations.size());
    }

    @Test
    public void testValidPerson() {
        Person person = new Person();
        person.setName("ZhangJianHe");
        person.setAge(20);
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<Person>> violations = validator.validate(person);
        Assertions.assertEquals(0, violations.size());
    }

}
