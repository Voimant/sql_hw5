import psycopg2
from psycopg2 import errors
from psycopg2 import sql


def create_table(conn):
    """Созднаие структуры базы"""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id_s SERIAL PRIMARY KEY,
            first_name VARCHAR(80) NOT NULL,
            last_name VARCHAR(80) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE
        );
        CREATE tABLE IF NOT EXISTS data_phone(
            id_s INTEGER REFERENCES users(id_s)
            ON DELETE CASCADE,
            phone VARCHAR(20) NOT NULL UNIQUE
        );
        """)
    return 'База данных создана'


def new_user(conn, f_name, l_name, e_mail, phone=""):
    """
    Добавление нового Юзера
    :param conn: Подключение к базе данных
    :param f_name: Имя пользователя
    :param l_name: Фамилия пользователя
    :param e_mail: Почта пользователя
    :param phone: Не обязательный аргумент, телефон пользователя
    :return: сообщение о внесении пользователя
    """
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO users(first_name, last_name, email)
        VALUES(%s,%s,%s);""", (f_name, l_name, e_mail))
        if phone != "":
            cur.execute("""
            SELECT id_s FROM users
            WHERE email = %s;""", (e_mail,))
            id_s = cur.fetchone()
            cur.execute("""
            INSERT INTO data_phone(id_s, phone)
            VALUES(%s,%s)""", (id_s, phone,))
            print('Новый пользователь и телефон добавлен')
        else:
            print('пользователь добавлен без телефона')


def add_phone(conn, e_mail, phone):
    """
    Добавление телефона существующему польователю
    :param conn: Подключение к базе данных
    :param e_mail: Почта существующего пользователя
    :param phone: Новый телефон пользователя
    :return: Сообщение о выполненной операции
    """
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id_s FROM users
        WHERE email = %s;""", (e_mail,))
        id_s = cur.fetchone()
        cur.execute("""
        INSERT INTO data_phone(id_s, phone)
        VALUES(%s, %s);""", (id_s, phone))
        print("Новый телефон добавлен")


def change_users(conn, e_mail, table, column, new_value):
    """
    Изменение данных о пользователе
    :param conn: подключение к базе данных
    :param e_mail: Почта существующего пользователя
    :param table: в какой таблице будут изменения
    :param column: в какой колонке требуют изменений
    :param new_value: Новое значение
    """
    with conn.cursor() as cur:
        cur.execute("""
               SELECT id_s FROM users
               WHERE email = %s;""", (e_mail,))
        id_s = cur.fetchone()
        update_query = sql.SQL("""UPDATE {table} SET {column} = '{new_value}' WHERE id_s = %s;""").format(
            table=sql.Identifier(table), column=sql.Identifier(column), new_value=sql.Identifier(new_value))
        cur.execute(update_query, id_s)
        print("Данные изменены")


def delete_phone(conn, e_mail):
    """
    Удаление телефона у существующего пользователя
    :param conn: Подключение к базе данных
    :param e_mail: почта существующего пользователя
    """
    with conn.cursor() as cur:
        cur.execute("""
               SELECT id_s FROM users
               WHERE email = %s;""", (e_mail,))
        id_s = cur.fetchone()
        cur.execute("""
        DELETE FROM data_phone
        WHERE id_s = %s;""", (id_s,))
        print("Телефоны пользователя удалены")


def delete_user(conn, e_mail):
    """
    Удаление существующего пользователя
    :param conn: подключение к базе данных
    :param e_mail: почта существующего пользователя
    """
    with conn.cursor() as cur:
        cur.execute("""
               SELECT id_s FROM users
               WHERE email = %s;""", (e_mail,))
        id_s = cur.fetchone()
        cur.execute("""
        DELETE FROM users
        WHERE email = %s;""", (e_mail,))
        print('Пользователь удален')


def find_user(conn, f_name=0, l_name=0, e_mail=0, phone=0):
    """
    Поиск пользователя
    :param conn: Подключение к базе данных
    :param f_name: Не обязательный аргумент, поиск по имени пользователей
    :param l_name: Не обязательный аргумент, поиск по фамилии пользователей
    :param e_mail: Не обязательный аргумент, поиск по почте пользователей
    :param phone: Не обязательный аргумент, поиск по телефону пользователя
    :return: Возвращает список кортежей записей из БД найденых пользователей
    """
    with conn.cursor() as cur:
        if f_name != 0:
            cur.execute("""
            SELECT * FROM users
            JOIN data_phone ON users.id_s = data_phone.id_s
            WHERE first_name = %s;""", (f_name,))
        elif l_name != 0:
            cur.execute("""
            SELECT * FROM users
            JOIN data_phone ON users.id_s = data_phone.id_s
            WHERE last_name = %s;""", (l_name,))

        elif e_mail != 0:
            cur.execute("""
            SELECT * FROM users
            JOIN data_phone ON users.id_s = data_phone.id_s
            WHERE email = %s;""", (e_mail,))

        elif phone != 0:
            cur.execute("""
            SELECT * FROM users
            JOIN data_phone ON users.id_s = data_phone.id_s
            WHERE phone = %s;""", (phone,))

        infos = cur.fetchall()
    return infos


if __name__ == "__main__":
    with psycopg2.connect(database="hw_5", user="postgres", password="voimant11") as conn:  # подключение к базе
        # Создание разметки таблицы, после первого включения, нужно закоментировать
        create_table(conn)
        # Наполнение базы, после внесения, закомментировать вызовы функций
        new_user(conn, 'Олег', 'Шерстобитов', 'voimant007@gmail.com', '89324617')
        new_user(conn, 'Люся', 'Ахметова', 'lusa@gmail.com', '234643547')
        new_user(conn, 'Таня', 'Щекатурова', 'mypka@ya.ru', '893254617')
        new_user(conn, 'Валенок', 'Телегов', 'neyasno@gmail.com', '34735454')
        # --------------------------------------------------------------------------
        # Добавление телефона пользователю, закоментировать послеиспользования
        add_phone(conn, "voimant007@gmail.com", "8915942977")
        # Изменение данных о пользователях
        change_users(conn, 'voimant007@gmail.com', 'users', 'last_name', 'sherr')
        # Удаление телефона у пользователя
        delete_phone(conn, 'voimant007@gmail.com')
        # Удаление пользователя
        delete_user(conn, 'voimant007@gmail.com')
        # Поиск пользователя
        print(find_user(conn, phone="34735454"))

        conn.commit()
