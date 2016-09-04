package org.infinispan.spark.domain;

import com.google.common.base.Objects;

import java.io.Serializable;

public class Book implements Serializable {

    private String title;
    private String description;
    private int publicationYear;
    private String author;

    public Book() {
    }

    public Book(String title, String description, int publicationYear, String author) {
        this.title = title;
        this.description = description;
        this.publicationYear = publicationYear;
        this.author = author;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getPublicationYear() {
        return publicationYear;
    }

    public void setPublicationYear(int publicationYear) {
        this.publicationYear = publicationYear;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Book book = (Book) o;
        return publicationYear == book.publicationYear &&
                Objects.equal(title, book.title) &&
                Objects.equal(description, book.description) &&
                Objects.equal(author, book.author);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(title, description, publicationYear, author);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Book{");
        sb.append("title='").append(title).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", publicationYear=").append(publicationYear);
        sb.append(", author='").append(author).append('\'');
        sb.append('}');
        return sb.toString();
    }
}