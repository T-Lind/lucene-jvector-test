package org.tlind;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

class Vector {
    String id;
    float[] values;

    Vector(String id, float[] values) {
        this.id = id;
        this.values = values;
    }
}

public class KNN {
    private List<Vector> vectors;

    public KNN() {
        this.vectors = new ArrayList<>();
    }

    public void loadVectors(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 3) {
                    String id = parts[0];
                    String[] vectorStrs = parts[2].split(",");
                    float[] vector = new float[vectorStrs.length];
                    for (int i = 0; i < vectorStrs.length; i++) {
                        vector[i] = Float.parseFloat(vectorStrs[i]);
                    }
                    vectors.add(new Vector(id, vector));
                }
            }
        }
    }

    public List<Vector> computeKNN(float[] queryVector, int k) {
        PriorityQueue<VectorDistance> pq = new PriorityQueue<>(k, (a, b) -> Float.compare(b.distance, a.distance));

        for (Vector vector : vectors) {
            float distance = calculateDistance(queryVector, vector.values);
            if (pq.size() < k) {
                pq.offer(new VectorDistance(vector, distance));
            } else if (distance < pq.peek().distance) {
                pq.poll();
                pq.offer(new VectorDistance(vector, distance));
            }
        }

        List<Vector> knn = new ArrayList<>();
        while (!pq.isEmpty()) {
            knn.add(pq.poll().vector);
        }

        return knn;
    }

    private float calculateDistance(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return (float) Math.sqrt(sum);
    }


    private static class VectorDistance {
        Vector vector;
        float distance;

        VectorDistance(Vector vector, float distance) {
            this.vector = vector;
            this.distance = distance;
        }
    }
}