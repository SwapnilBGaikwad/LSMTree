package org.example;

import lombok.Builder;
import lombok.Data;

public class Main {
    public static void main(String[] args) {
        Database<String, User> database = new HBaseDatabase<>();
        User user1 = User.builder()
                .id("User-1")
                .name("name")
                .password("password")
                .build();
        User user2 = User.builder()
                .id("User-2")
                .name("name")
                .password("password")
                .build();
        database.put(user1.getId(), user1);
        database.put(user2.getId(), user2);
        System.out.println(database.get("User-1"));
        System.out.println(database.scan("User"));
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