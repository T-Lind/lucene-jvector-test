package org.tlind;
import java.util.List;

public class SimplifiedData {
    private List<TitleEmbPair> pairs;

    public SimplifiedData(List<TitleEmbPair> pairs) {
        this.pairs = pairs;
    }

    // Getters and setters
    public List<TitleEmbPair> getPairs() {
        return pairs;
    }

    public void setPairs(List<TitleEmbPair> pairs) {
        this.pairs = pairs;
    }
}
