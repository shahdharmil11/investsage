package com.dharmil.investsage.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                // Allow requests from the Databutton development UI origin
                // Find your specific Databutton preview origin if different
                registry.addMapping("/api/**") // Allow CORS for /api paths
                        .allowedOrigins("https://databutton.com") // Allow Databutton domain
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS") // Allowed HTTP methods
                        .allowedOrigins("http://localhost:3000") // Allow localhost for development
                        .allowedHeaders("*") // Allow all headers
                        .allowCredentials(true); // If you need cookies/auth headers later
            }
        };
    }
}
