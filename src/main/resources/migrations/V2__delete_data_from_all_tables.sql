DELETE FROM books;
DELETE FROM authors;
DELETE FROM genres;

ALTER SEQUENCE books_id_seq RESTART WITH 1;
ALTER SEQUENCE authors_id_seq RESTART WITH 1;
ALTER SEQUENCE genres_id_seq RESTART WITH 1;