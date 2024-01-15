import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale


def maxlen(x):
    return max(map(lambda y: len(str(y)), x))


def complete_tables(session, data):
    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
        session.commit()


def info_publisher(name):
    q = session.query(Sale).join(
        Stock, Sale.id_stock == Stock.id
    ).join(
        Shop, Stock.id_shop == Shop.id
    ).join(
        Book, Stock.id_book == Book.id
    ).join(
        Publisher, Book.id_publisher == Publisher.id
    ).filter(
        Publisher.name == name
    )
    books = []
    shops = []
    prices = []
    sale_dates = []
    for sale in q.all():
        books.append(sale.stock.book.title)
        shops.append(sale.stock.shop.name)
        prices.append(sale.price)
        sale_dates.append('-'.join(str(sale.date_sale).split()[0].split('-')[::-1]))
    ans = [
        ' | '.join(list(map(lambda x: f"{x[i]: <{maxlen(x)}}", (books, shops, prices, sale_dates))))
        for i, _ in enumerate(books)
    ]
    return ans


if __name__ == "__main__":
    with open('DSN.json', 'r') as f:  # Для работы заполните файл DSN.json
        dsn = json.load(f)
        DSN = f"{dsn['driver']}://{dsn['login']}:{dsn['password']}@{dsn['host']}:{dsn['port']}/{dsn['data_base_name']}"
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    with open('fixtures/tests_data.json', 'r') as fd:
        data = json.load(fd)

    complete_tables(session, data)  # Если возникает ошибка о уже существующих данных, закомментируйте эту строку

    print('\n'.join(info_publisher(input())))
