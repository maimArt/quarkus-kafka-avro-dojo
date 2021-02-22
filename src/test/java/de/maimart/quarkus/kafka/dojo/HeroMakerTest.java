package de.maimart.quarkus.kafka.dojo;

import de.maimart.avro.Hero;
import de.maimart.avro.Somebody;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;

@QuarkusTest
class HeroMakerTest {

    static String SOMEBODY_TOPIC = "reactive.somebodies";
    static String HEROS_TOPIC = "reactive.heroes";

    @Test
    void somebodiesAreMadeToHeros() {
        Somebody somebody = new Somebody("John", "Snow");

        Hero hero = null;
        assertThat(hero).isNotNull();
        assertThat(hero.getSword()).isEqualTo("Longclaw");
    }
}