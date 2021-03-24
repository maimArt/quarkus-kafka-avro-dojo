package de.maimart.quarkus.kafka.dojo;

import de.maimart.avro.Hero;
import de.maimart.avro.Somebody;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

public class HeroMaker {

    public static String SOMEBODY_TOPIC = "streams.somebody-topic";
    public static String HEROS_TOPIC = "streams.hero-topic";
    private final Armory armory;
    private final AvroSerdeFactory avroSerdeFactory;

    @Inject
    public HeroMaker(Armory armory, AvroSerdeFactory avroSerdeFactory) {
        this.armory = armory;
        this.avroSerdeFactory = avroSerdeFactory;
    }

    @Produces
    public Topology makeSomebodyToAHero() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(SOMEBODY_TOPIC, Consumed.with(Serdes.String(), avroSerdeFactory.buildAvroValueSerde(Somebody.class)))
                .mapValues((key, somebody) -> somebodyToHero(somebody))
                .to(HEROS_TOPIC, Produced.with(Serdes.String(), avroSerdeFactory.buildAvroValueSerde(Hero.class)));
        return streamsBuilder.build();
    }

    private Hero somebodyToHero(Somebody somebody) {
        return new Hero(somebody.getPrename(), somebody.getSurname(), armory.getHerosWeapon(somebody.getPrename(), somebody.getSurname()));
    }
}