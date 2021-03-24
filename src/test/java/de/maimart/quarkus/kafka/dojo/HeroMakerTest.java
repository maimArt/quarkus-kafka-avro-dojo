package de.maimart.quarkus.kafka.dojo;

import de.id.quarkus.kafka.testing.ConfluentStack;
import de.id.quarkus.kafka.testing.ConfluentStackClient;
import de.maimart.avro.Hero;
import de.maimart.avro.Somebody;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.maimart.quarkus.kafka.dojo.HeroMaker.*;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(ConfluentStack.class)
class HeroMakerTest {

    ConfluentStackClient client;
    Armory armory;

    @BeforeEach
    void setUp() {
        client.createTopics(SOMEBODY_TOPIC, HEROS_TOPIC);
        HashMap<String, String> armoryTopicConfigs = new HashMap<>();
        armoryTopicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        NewTopic armoryTopic = new NewTopic(ARMORY_TOPIC, 1, (short) 1)
                .configs(armoryTopicConfigs);
        client.createAdminClient().createTopics(Collections.singletonList(armoryTopic));
        armory = new Armory();
    }

    @Test
    void somebodiesAreMadeToHeroes() throws InterruptedException, ExecutionException, TimeoutException {
        client.sendRecords(ARMORY_TOPIC, armory.getAllWeapons(), StringSerializer.class, (integer, weapon) -> armory.getOwner(weapon));

        Somebody somebody = new Somebody("John", "Snow");

        Future<List<Hero>> futureHeroes = client.waitForRecords(HEROS_TOPIC, "heroes-consumer", 1, StringDeserializer.class);

        client.sendRecords(SOMEBODY_TOPIC, singletonList(somebody), StringSerializer.class, (index, sb) -> sb.getPrename() + " " + sb.getSurname());

        List<Hero> receivedHeroes = futureHeroes.get(5, TimeUnit.SECONDS);

        Hero hero = receivedHeroes.get(0);
        assertThat(hero).isNotNull();
        assertThat(hero.getPrename()).isEqualTo(somebody.getPrename());
        assertThat(hero.getSurname()).isEqualTo(somebody.getSurname());
        assertThat(hero.getSword()).isEqualTo("Longclaw");
    }
}