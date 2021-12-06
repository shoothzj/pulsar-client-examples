package com.github.shoothzj.pulsar.client.examples.module;

import lombok.Data;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

@Data
public class Person {

    @NotNull
    private String name;

    @Min(value = 18)
    @Max(value = 60)
    private int age;

    public Person() {
    }
}
