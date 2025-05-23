package com.dharmil.investsage.config;

import akka.actor.ActorSystem;
import com.dharmil.investsage.akka.di.SpringAkkaExtension;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException; // <<< Import ConfigException
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AkkaConfig {

    private static final Logger log = LoggerFactory.getLogger(AkkaConfig.class);

    @Autowired
    private ApplicationContext applicationContext;

    // Define the fully qualified dispatcher path as a constant
    private static final String BLOCKING_IO_DISPATCHER_PATH = "akka.actor.dispatchers.blocking-io-dispatcher";

    @Bean(destroyMethod = "terminate")
    public ActorSystem actorSystem() {
        log.info("Initializing Akka ActorSystem...");

        // Explicitly load application.conf and merge with defaults
        final Config appConfig = ConfigFactory.parseResources("application.conf");
        final Config referenceConfig = ConfigFactory.defaultReference();
        final Config finalConfig = appConfig.withFallback(referenceConfig);
        final Config resolvedConfig = finalConfig.resolve();

        // *** UPDATED CHECK: Try to get the config object directly ***
        try {
            // Attempt to retrieve the specific configuration block
            Config dispatcherConfig = resolvedConfig.getConfig(BLOCKING_IO_DISPATCHER_PATH);
            log.info("SUCCESS: Found and retrieved config for '{}'. Type: {}", BLOCKING_IO_DISPATCHER_PATH, dispatcherConfig.getString("type")); // Log type as confirmation
        } catch (ConfigException.Missing e) {
            // This exception is thrown if the path doesn't exist
            log.error("!!! FAILURE: Path '{}' is MISSING in the resolved configuration. Check application.conf. !!!", BLOCKING_IO_DISPATCHER_PATH);
            // Log the full config again for debugging if missing
            try {
                ConfigRenderOptions renderOpts = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false);
                log.error("Full Resolved Configuration:\n---\n{}\n---", resolvedConfig.root().render(renderOpts));
            } catch (Exception renderEx) {
                log.error("Error rendering config during error handling", renderEx);
            }
            throw new RuntimeException("Missing required dispatcher configuration: " + BLOCKING_IO_DISPATCHER_PATH, e);
        } catch (Exception e) {
            // Catch other potential errors during getConfig
            log.error("!!! FAILURE: Unexpected error retrieving config for '{}' !!!", BLOCKING_IO_DISPATCHER_PATH, e);
            throw new RuntimeException("Error retrieving dispatcher configuration: " + BLOCKING_IO_DISPATCHER_PATH, e);
        }
        // *** END UPDATED CHECK ***


        // Create the ActorSystem using the explicitly resolved config
        final String actorSystemName = "InvestsageAkkaSystem";
        final ActorSystem system;
        try {
            system = ActorSystem.create(actorSystemName, resolvedConfig);
            log.info("Akka ActorSystem '{}' created.", system.name());
        } catch (Exception e) {
            log.error("!!! Failed to create ActorSystem !!!", e);
            throw new RuntimeException("ActorSystem creation failed", e);
        }


        // Check dispatcher lookup AFTER system creation (keep this check)
        try {
            system.dispatchers().lookup(BLOCKING_IO_DISPATCHER_PATH);
            log.info("SUCCESS: Dispatcher '{}' lookup successful immediately after ActorSystem creation.", BLOCKING_IO_DISPATCHER_PATH);
        } catch (akka.ConfigurationException ce) {
            log.error("!!! FAILURE: Dispatcher '{}' lookup FAILED immediately after ActorSystem creation: {} !!!", BLOCKING_IO_DISPATCHER_PATH, ce.getMessage());
            system.terminate();
            throw new RuntimeException("Dispatcher lookup failed during system initialization", ce);
        } catch (Exception e) {
            log.error("!!! FAILURE: Unexpected error during immediate dispatcher lookup for '{}' !!!", BLOCKING_IO_DISPATCHER_PATH, e);
            system.terminate();
            throw new RuntimeException("Unexpected dispatcher lookup error", e);
        }


        // Initialize the SpringAkkaExtension
        log.info("Initializing SpringAkkaExtension with ApplicationContext...");
        try {
            SpringAkkaExtension.provider.get(system).initialize(applicationContext);
            log.info("SpringAkkaExtension initialized successfully.");
        } catch (Exception e) {
            log.error("Failed to initialize SpringAkkaExtension!", e);
            system.terminate();
            throw new RuntimeException("Could not initialize SpringAkkaExtension", e);
        }

        return system;
    }
}
