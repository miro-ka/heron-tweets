package org.warnup.heron.tweets.internal;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.warnup.heron.tweets.dto.ConfigItemDto;
import org.warnup.heron.tweets.dto.InputConfigDto;

import java.io.FileWriter;
import java.io.FileReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;


public class JsonConfigParser {

    final private String configName = "config.json";

    public JsonConfigParser() {
    }


    public InputConfigDto read() {

        InputConfigDto configData = null;

        try{
            Gson gson = new Gson();
            JsonReader reader = new JsonReader(new FileReader(configName));
            configData = gson.fromJson(reader, InputConfigDto.class);
        }catch (Exception e){
            System.out.println("Exception thrown  :" + e);
        }

        return configData;
    }


    private void createTestConfig() {

        ConfigItemDto configItem1 = new ConfigItemDto();
        configItem1.streamItemId = 1;
        configItem1.streamkKeywords = new String[]{"news", "breaking", "finance"};
        configItem1.twitterIds = new String[]{"22100200", "4433322212"};

        ConfigItemDto configItem2 = new ConfigItemDto();
        configItem2.streamItemId = 2;
        configItem2.streamkKeywords = new String[]{"tech", "science"};
        configItem2.twitterIds = new String[]{"444555", "77788811"};

        List<ConfigItemDto> items = new ArrayList<>();
        items.add(configItem1);
        items.add(configItem2);

        InputConfigDto config = new InputConfigDto();
        config.items = items;


        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        System.out.println(gson.toJson(config));


        try (Writer writer = new FileWriter(configName)) {
            gson.toJson(config, writer);
        } catch (Exception e){
            System.out.println("Exception thrown  :" + e);
        }
    }
}
