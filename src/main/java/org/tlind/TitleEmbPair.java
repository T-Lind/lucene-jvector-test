package org.tlind;


import java.util.concurrent.ConcurrentLinkedQueue;

public class TitleEmbPair {
    private String title;
    private ConcurrentLinkedQueue<Float> emb;

    public TitleEmbPair(String title, ConcurrentLinkedQueue<Float> emb) {
        this.title = title;
        this.emb = emb;
    }

    // Getters and setters
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public ConcurrentLinkedQueue<Float> getEmb() {
        return emb;
    }

    public void setEmb(ConcurrentLinkedQueue<Float> emb) {
        this.emb = emb;
    }
}