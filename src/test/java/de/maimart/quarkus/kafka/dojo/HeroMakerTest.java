package de.maimart.quarkus.kafka.dojo;

import de.id.quarkus.kafka.testing.ConfluentStack;
import de.id.quarkus.kafka.testing.ConfluentStackClient;
import de.maimart.avro.Hero;
import de.maimart.avro.Somebody;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.maimart.quarkus.kafka.dojo.HeroMaker.HEROS_TOPIC;
import static de.maimart.quarkus.kafka.dojo.HeroMaker.SOMEBODY_TOPIC;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(ConfluentStack.class)
class HeroMakerTest {

    ConfluentStackClient client;

    @BeforeEach
    void setUp() {
        client.createTopics(SOMEBODY_TOPIC, HEROS_TOPIC);
    }

    @Test
    void somebodiesAreMadeToHeroes() throws InterruptedException, ExecutionException, TimeoutException {
        Somebody somebody = new Somebody("John", "Snow");

        Future<List<Hero>> futureHeroes = client.waitForRecords(HEROS_TOPIC, "heroes-consumer", 1, StringDeserializer.class);

        client.sendRecords(SOMEBODY_TOPIC, singletonList(somebody), StringSerializer.class, (index, sb) -> sb.getPrename() + sb.getSurname());

        List<Hero> receivedHeroes = futureHeroes.get(5, TimeUnit.SECONDS);

        Hero hero = receivedHeroes.get(0);
        assertThat(hero).isNotNull();
        assertThat(hero.getPrename()).isEqualTo(somebody.getPrename());
        assertThat(hero.getSurname()).isEqualTo(somebody.getSurname());
        assertThat(hero.getSword()).isEqualTo("Longclaw");
    }
}