package org.tlind;

import java.util.List;

public class TitleEmbPair {
    private String title;
    private List<Double> emb;

    public TitleEmbPair(String title, List<Double> emb) {
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

    public List<Double> getEmb() {
        return emb;
    }

    public void setEmb(List<Double> emb) {
        this.emb = emb;
    }

    @Override
    public String toString() {
        return "TitleEmbPair{" +
                "title='" + title + '\'' +
                ", emb=" + emb +
                '}';
    }
}