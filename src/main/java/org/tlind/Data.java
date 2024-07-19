package org.tlind;
import java.util.List;

public class Data {
    private List<String> _id;
    private List<String> url;
    private List<String> title;
    private List<String> text;
    private List<List<Double>> emb;

    // Getters and setters

    public List<String> get_id() {
        return _id;
    }

    public void set_id(List<String> _id) {
        this._id = _id;
    }

    public List<String> getUrl() {
        return url;
    }

    public void setUrl(List<String> url) {
        this.url = url;
    }

    public List<String> getTitle() {
        return title;
    }

    public void setTitle(List<String> title) {
        this.title = title;
    }

    public List<String> getText() {
        return text;
    }

    public void setText(List<String> text) {
        this.text = text;
    }

    public List<List<Double>> getEmb() {
        return emb;
    }

    public void setEmb(List<List<Double>> emb) {
        this.emb = emb;
    }
}
