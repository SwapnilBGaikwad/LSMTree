package org.example;

import lombok.Builder;
import lombok.Data;

public class Main {
    public static void main(String[] args) {
        Database<String, User> database = new HBaseDatabase<>();
        User user = User.builder()
                .id("1")
                .name("name")
                .password("password")
                .build();
        database.put("1", user);
        System.out.println(database.get("1"));
    }

    @Data
    static class User {
        private String id;
        private String name;
        private String password;

        @Builder
        public User(String id, String name, String password) {
            this.id = id;
            this.name = name;
            this.password = password;
        }
    }
}