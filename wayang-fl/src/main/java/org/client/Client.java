package org.client;

public class Client {
    private final String url;
    private final String name;

    public Client(String url, String name){
        this.url = url;
        this.name = name;
    }

    public String getUrl(){
        return url;
    }

    public String getName(){
        return name;
    }
}
