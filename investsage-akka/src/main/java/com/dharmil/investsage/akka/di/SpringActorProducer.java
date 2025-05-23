package com.dharmil.investsage.akka.di;

import akka.actor.Actor;
import akka.actor.IndirectActorProducer;
import org.springframework.context.ApplicationContext;

/**
 * An Akka IndirectActorProducer that delegates the creation of actors
 * to the Spring ApplicationContext.
 *
 * This allows actors to be defined as Spring beans (typically with prototype scope)
 * and have their dependencies injected by Spring before being used by Akka.
 */
public class SpringActorProducer implements IndirectActorProducer {

    private final ApplicationContext applicationContext;
    private final String actorBeanName;
    private final Object[] args; // Arguments to pass to the bean factory method if needed

    /**
     * Constructor.
     * @param applicationContext The Spring ApplicationContext.
     * @param actorBeanName The name of the Spring bean defined for the actor.
     * @param args Optional arguments to pass to ApplicationContext.getBean() if the
     * bean definition requires constructor args not supplied by Spring DI.
     */
    public SpringActorProducer(ApplicationContext applicationContext, String actorBeanName, Object... args) {
        this.applicationContext = applicationContext;
        this.actorBeanName = actorBeanName;
        this.args = args; // Store arguments
    }

    /**
     * This method is called by Akka to produce the actor instance.
     * It retrieves a bean instance from the Spring context.
     * Assumes the bean is defined with prototype scope to ensure a new instance
     * is created each time (important for actor restarts).
     * @return A new Actor instance created and managed by Spring.
     */
    @Override
    public Actor produce() {
        // Use getBean with arguments - Spring will still perform autowiring for other dependencies
        // defined in the bean's class, but these args can satisfy explicit constructor parameters.
        if (args == null || args.length == 0) {
            // If no specific args provided, use the simpler getBean method
            return (Actor) applicationContext.getBean(actorBeanName);
        } else {
            // If args are provided, pass them to getBean
            return (Actor) applicationContext.getBean(actorBeanName, args);
        }
    }

    /**
     * This method is called by Akka to determine the class of the actor being produced.
     * It retrieves the bean type from the Spring context.
     * @return The Class of the actor bean.
     */
    @Override
    @SuppressWarnings("unchecked") // Suppress warning for casting Class<?> to Class<? extends Actor>
    public Class<? extends Actor> actorClass() {
        return (Class<? extends Actor>) applicationContext.getType(actorBeanName);
    }
}
