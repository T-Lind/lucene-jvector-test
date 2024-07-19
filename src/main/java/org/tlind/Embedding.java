package org.tlind;

import java.util.List;

public class Embedding {
    private List<Double> emb;

    public List<Double> getEmb() {
        return emb;
    }

    public void setEmb(List<Double> emb) {
        this.emb = emb;
    }

    public Embedding(List<Double> emb) {
        this.emb = emb;
    }

    public Embedding() {}
}
