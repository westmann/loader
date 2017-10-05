package com.couchbase.bigfun;

import java.util.Arrays;
import java.util.ArrayList;

public class Book {

    private String[] authors;
    private String isbn10;
    private String isbn13;
    private String title;
    private Object o;
    private long number;
    private ArrayList<String> vs = new ArrayList<String>();
    public Book(String author, String isbn10, String isbn13, String title, long number)
    {
        this.authors = new String[1];
        this.authors[0] = author;
        this.isbn10 = isbn10;
        this.isbn13 = isbn13;
        this.title = title;
        this.o = 100;
        this.number = number;
        vs.add("ab");

    }

    @Override
    public boolean equals(Object o) {

        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }

        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof Book)) {
            return false;
        }

        // typecast o to Complex so that we can compare data members
        Book c = (Book) o;

        // Compare the data members and return accordingly
        return title.equals(c.title) && (number == c.number) && isbn10.equals(c.isbn10)
                && isbn13.equals(c.isbn13) && Arrays.deepEquals(authors, c.authors);

    }
}
