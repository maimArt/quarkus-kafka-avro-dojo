package de.maimart.quarkus.kafka.dojo;

import de.maimart.avro.Weapon;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Armory {

    private final Map<String, String> swordsOfHeros;

    public Armory() {
        this.swordsOfHeros = new HashMap<>();
        this.swordsOfHeros.put("John Snow", "Longclaw");
        this.swordsOfHeros.put("Aria Stark", "Needle");
        this.swordsOfHeros.put("Eduard Stark", "Ice");
    }

    public List<Weapon> getAllWeapons() {
        return swordsOfHeros.values().stream()
                .map(weaponName -> new Weapon(weaponName, 0L)).collect(Collectors.toList());
    }

    public String getOwner(Weapon weapon) {
        return swordsOfHeros.entrySet().stream()
                .filter(entry -> entry.getValue().equals(weapon.getName()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse("");
    }
}
