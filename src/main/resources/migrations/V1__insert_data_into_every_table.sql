INSERT INTO authors (id, name) VALUES
(1, 'Джон Р. Р. Толкин'),
(2, 'Артур Конан Дойл'),
(3, 'Уильям Шекспир'),
(4, 'Жюль Верн'),
(5, 'Брэм Стокер');

INSERT INTO genres (id, name) VALUES
(1, 'Фантастика'),
(2, 'Детектив'),
(3, 'Романтика'),
(4, 'Приключения'),
(5, 'Ужасы');

INSERT INTO books (id, title, author_id, genre_id, rating) VALUES
(1, 'Властелин колец', 1, 1, 4.9),
(2, 'Шерлок Холмс: Этюд в багровых тонах', 2, 2, 4.7),
(3, 'Ромео и Джульетта', 3, 3, 4.8),
(4, 'Путешествие к центру Земли', 4, 4, 4.6),
(5, 'Дракула', 5, 5, 4.5);