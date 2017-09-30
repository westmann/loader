package com.couchbase.bigfun;

import java.util.Random;

public class RandomCategoryGen {
    private String categories[];
    private int propotions[];
    private int upperBound[];
    private Random random;
    public RandomCategoryGen(String categories[], int propotions[]) {
        if (categories == null || propotions == null || categories.length != propotions.length || categories.length == 0)
            throw new IllegalArgumentException("Invalid input parameter for constructing RandomCategoryGen");
        this.categories = categories;
        this.propotions = propotions;
        this.upperBound = new int[propotions.length];
        for (int i = 0; i < propotions.length; i++) {
            if (i == 0)
                this.upperBound[i] = this.propotions[i];
            else
                this.upperBound[i] = this.propotions[i] + this.upperBound[i-1];
        }
        this.random = new Random();
    }
    public String nextCategory() {
        int v = random.nextInt(this.upperBound[this.upperBound.length - 1]);
        for (int i = 0; i < this.upperBound.length; i++) {
            if (v < this.upperBound[i])
                return this.categories[i];
        }
        throw new IllegalArgumentException("Invalid state for RandomCategoryGen");
    }
}
