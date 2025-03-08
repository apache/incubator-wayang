package org.server;

public class Server {
    private final String url;
    private final String name;

    public Server(String url, String name){
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
