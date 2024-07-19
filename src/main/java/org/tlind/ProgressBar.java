package org.tlind;

public class ProgressBar {
    private final int total;
    private int progress;

    public ProgressBar(int total) {
        this.total = total;
        this.progress = 0;
    }

    public synchronized void update() {
        progress++;
        printProgress();
    }

    private void printProgress() {
        int barLength = 50;
        int completedLength = (int) (((double) progress / total) * barLength);
        StringBuilder bar = new StringBuilder("[");
        bar.append("=".repeat(Math.max(0, completedLength)));
        bar.append(" ".repeat(Math.max(0, barLength - completedLength)));
        bar.append("] ").append(progress).append("/").append(total);
        System.out.print("\r" + bar.toString());
    }
}