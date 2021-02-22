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

    public String getHerosWeapon(String prename, String surname) {
        String completeName = String.format("%s %s", prename, surname);
        return swordsOfHeros.getOrDefault(completeName, "Dagger");
    }
}
