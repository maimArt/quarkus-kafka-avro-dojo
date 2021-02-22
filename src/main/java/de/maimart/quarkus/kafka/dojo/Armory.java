package de.maimart.quarkus.kafka.dojo;

import javax.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;

public class Armory {

    private final Map<String, String> swordsOfHeros;

    public Armory() {
        this.swordsOfHeros = new HashMap<>();
        this.swordsOfHeros.put("John Snow", "Longclaw");
        this.swordsOfHeros.put("Aria Stark", "Needle");
        this.swordsOfHeros.put("Eduard Stark", "Ice");
    }

    public String getHerosWeapon(String hero) {
        return swordsOfHeros.getOrDefault(hero, "Dagger");
    }
}
