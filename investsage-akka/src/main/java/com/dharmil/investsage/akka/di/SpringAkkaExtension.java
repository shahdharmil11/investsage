package com.dharmil.investsage.akka.di;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.Props;
import org.springframework.context.ApplicationContext;

/**
 * An Akka Extension to integrate with Spring ApplicationContext, enabling the creation
 * of Spring-managed actor beans that have dependencies injected.
 *
 * Usage:
 * 1. Initialize the extension once per ActorSystem:
 * SpringAkkaExtension.provider().get(actorSystem).initialize(applicationContext);
 *
 * 2. Create Props for Spring-managed actors using the extension:
 * Props props = SpringAkkaExtension.provider().get(actorSystem).props("myActorBeanName", arg1, arg2);
 * actorSystem.actorOf(props, "myActorInstance");
 */
public class SpringAkkaExtension extends AbstractExtensionId<SpringAkkaExtension.SpringExt> {

    // Singleton instance of the ExtensionId provider
    public static final SpringAkkaExtension provider = new SpringAkkaExtension();

    // Private constructor to prevent direct instantiation
    private SpringAkkaExtension() {}

    /**
     * Creates the actual Extension instance (SpringExt) for the given ActorSystem.
     * This is called by Akka only once per ActorSystem.
     * @param system The actor system.
     * @return The SpringExt instance.
     */
    @Override
    public SpringExt createExtension(ExtendedActorSystem system) {
        return new SpringExt();
    }

    /**
     * The actual Akka Extension implementation.
     * Holds the Spring ApplicationContext and provides the factory method for Props.
     */
    public static class SpringExt implements Extension {

        private volatile ApplicationContext applicationContext;

        /**
         * Initializes the extension with the Spring ApplicationContext.
         * This method must be called manually once after the ActorSystem and
         * ApplicationContext have been created.
         * @param applicationContext The Spring ApplicationContext.
         */
        public void initialize(ApplicationContext applicationContext) {
            // Check if already initialized
            if (this.applicationContext != null) {
                // Log a warning or handle as appropriate if re-initialization is attempted
                System.err.println("WARN: SpringAkkaExtension already initialized. Ignoring subsequent call.");
                return;
            }
            if (applicationContext == null) {
                throw new IllegalArgumentException("ApplicationContext cannot be null during SpringAkkaExtension initialization.");
            }
            this.applicationContext = applicationContext;
            System.out.println("INFO: SpringAkkaExtension initialized with ApplicationContext."); // Use logger in real app
        }

        /**
         * Creates Akka Props for a Spring-managed actor bean.
         * The actor bean MUST be defined with prototype scope in Spring configuration.
         *
         * @param actorBeanName The name of the Spring bean defined for the actor.
         * @param args Optional arguments to pass to ApplicationContext.getBean() if the
         * bean definition requires constructor args not supplied by Spring DI.
         * These arguments are passed AFTER any autowired constructor arguments.
         * @return Props configured to create the actor using Spring.
         * @throws IllegalStateException if the extension has not been initialized.
         */
        public Props props(String actorBeanName, Object... args) {
            if (applicationContext == null) {
                throw new IllegalStateException("SpringAkkaExtension has not been initialized. Call initialize(applicationContext) first.");
            }
            // Create Props using our SpringActorProducer
            return Props.create(SpringActorProducer.class, this.applicationContext, actorBeanName, args);
        }
    }
}
