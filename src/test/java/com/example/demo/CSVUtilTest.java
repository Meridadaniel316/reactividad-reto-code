package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }


    @Test //TEST PARA VER SI SI FILTRA A TODOS LOS JUGADORES MAYORES DE 35 AÑOS, TODO FUNCIONA CORRECTO
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .distinct()
                .collectMultimap(Player::getClub);

        // ============================== TEST VISUAL
        Objects.requireNonNull(listFilter.block()).forEach((clubTeam, players) -> {
            System.out.println("====================");
            System.out.println("Equipo: " + (clubTeam.isEmpty() ? "DESCONOCIDO" : clubTeam));
            System.out.println("====================");
            players.forEach(player -> {
                System.out.println(player.name +" |-| "+ player.age);
            });
        });
        assert Objects.requireNonNull(listFilter.block()).size() == 322;
    }

    @Test //TEST PARA FILTRAR JUGADORES MAYORES A 34 AÑOS, POR EQUIPOS, TODO FUNCIONA CORRECTAMENTE
    void reactive_filtrarJugadoresMayores34(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.club.equals(""))
                .filter(player -> player.age >= 34)
                .distinct()
                .collectMultimap(Player::getClub);

        // ============================== TEST VISUAL
        Objects.requireNonNull(listFilter.block()).forEach((clubTeam, players) -> {
            System.out.println("Equipo: " + (clubTeam.isEmpty() ? "DESCONOCIDO" : clubTeam));
            players.forEach(player -> {
                System.out.println(player.name +" |-| "+ player.age);
            });
        });
        assert Objects.requireNonNull(listFilter.block()).size() == 1;
    }

    @Test
    void reactive_filtrarNacionalidadYVictoriasJugadores() {
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .buffer(100)
                .flatMap(playerA -> listFlux
                        .filter(playerB -> playerA.stream()
                                .anyMatch(a -> a.national.equals(playerB.national)))
                ).distinct()
                .sort((country,player)-> player.winners)
                .collectMultimap(Player::getNational);

        Objects.requireNonNull(listFilter.block()).forEach((country, players)->{
            System.out.println("====================");
            System.out.println("PAIS: " + (country.isEmpty() ? "DESCONOCIDO" : country));
            System.out.println("====================");
            players.forEach(player -> {
                System.out.println(player.name + " |-| "+player.winners);
            });
        });

    }
}
